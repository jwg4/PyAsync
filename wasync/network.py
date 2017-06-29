import socket as S
from collections import namedtuple
__version__ = '$Rev$'

Hooks = namedtuple('Hooks','on_connection_accepted, on_data_received')

class CharacterServer:

    def __init__(self,host,port,hooks,teardown):
        self.host = host
        self.port = port
        self.hooks = hooks
        self._s = S.socket(S.AF_INET, S.SOCK_STREAM)
        self._terminated = False
        def terminate(ignored=True):
            print 'Terminating'
            self._terminated=True
            print 'sending quit signals to data loops'
            [x.abort() for x in self._data_loops]
            print 'sending quit signal to main loop'
            self._loop.abort()
            print 'sent quit signals'
        teardown.chain(terminate)
        print 'termination trigger ready'

    def go(self):
        #just let any exceptions bubble up
        bind_d = deferred(lambda host=self.host, port=self.port: self._s.bind((host,port)))
        listen_d = bind_d.chain(lambda _:self._s.listen(1))
        #let it run in the background
        self._loop = listen_d.chain(self.infinite_loop)
        self._loop.chain(lambda _ : self._s.close())
        def w(x):
            print x
        self._loop.chain(lambda _ : w("main loop has finished"))
        return self._loop

    def infinite_loop(self,ignored=True):
        self._data_loops = []
        while not self._terminated:
            (conn, addr) = self._s.accept()
            #let it run in the background
            self.hooks.on_connection_accepted()
            child_loop = deferred(lambda conn=conn,addr=addr: self.data_loop(conn,addr))
            child_loop.chain(lambda _,conn=conn: conn.close()) #we force a wait to catch exceptions
            self._data_loops.append(child_loop)
        await_all(self._data_loops)
        return True
    
    def data_loop(self,conn,addr):
        state = {'last': determined(1), 'conn': conn}
        while not self._terminated:
            def consume(data):
                if not data or data is None:
                    return False
                def reply(result):
                    (response,quit) = result
                    #never lose track of pending operations
                    if(response is not None):
                        state['last'] = state['last'].chain(lambda _, response=response: state['conn'].sendall(response + "\n"))
                    return quit
                response = self.hooks.on_data_received(data)
                quit = bind_or_apply(response,reply)
                return not quit
            def get():
                data = state['conn'].recv(1024)
                if not data:
                    return False
                return data
            data = deferred(get)
            if not data.bind(consume):
                #wait for all pending operations
                await(state['last'])
                break
            else:
                await(data)
        return state['conn']

#use this instead of direct class instantiation
def characterserver(host,port,hooks,teardown):
    r = deferred(lambda host=host,port=port,hooks=hooks,teardown=teardown: CharacterServer(host,port,hooks,teardown))
    return r
