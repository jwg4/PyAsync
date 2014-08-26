# $Id: scheduler.py 37042 2014-08-18 13:37:07Z alanm $
import time,sys
import threading
import Queue as SyncQueue

#we absolutely want all instances of wasync.Scheduler to talk to the same concurrent.futures
MIN_THREADS = 20
MAX_THREADS = 500
POLLING_CYCLE = 0.01
THREAD_COLLECTION_CYCLE = 15

class Worker(threading.Thread):
    def __init__(self,scheduler):
        threading.Thread.__init__(self)
        self.wq = SyncQueue.Queue()
        self.running = True
        self.scheduler = scheduler

    def run(self):
        while self.running:
            if not self.wq.empty():
                while not self.wq.empty():
                    f = self.wq.get(True,POLLING_CYCLE)
                    f()
                self.scheduler._idle_threads.put(self)
            time.sleep(POLLING_CYCLE)

class ThreadCollection(threading.Thread):
    def __init__(self,context,garbage,timeout = 30):
        threading.Thread.__init__(self)
        self.garbage = garbage
        self.context = context
        self.timeout = 30
        self.running = True

    def run(self):
        while self.running:
            self.context.log("thread collector: {0} threads to be collected".format(self.garbage.qsize()))
            while not self.garbage.empty():
                t = self.garbage.get(True)
                t.join(self.timeout)
                if t.isAlive():
                    #maybe later
                    self.garbage.put(t)
            time.sleep(THREAD_COLLECTION_CYCLE)

class MainLoop(threading.Thread):
    def __init__(self,context):
        threading.Thread.__init__(self)
        self.context = context

    def run(self):
        while self.context._running:
            #uncommenting the following is seriously spammy
            self.context.log("main loop: {0} slots, and {1} threads available".format(self.context._slots,self.context._idle_threads.qsize()))
            if self.context.can_run_job():
                self.context.log("main loop: can run a job")
                try:
                    job = self.context._job_queue.get(True,POLLING_CYCLE)
                    self.context.log("main loop: picked a job")
                    put_me_back = []
                    while job.is_blocked():
                        #put it back and get a new one
                        self.context.log("main loop: this job is blocked, pick another")
                        put_me_back.append(job)
                        try:
                            job = self.context._job_queue_get(True,POLLING_CYCLE)
                        except SyncQueue.Empty:
                            self.context.log("main loop: exhausted all possible jobs and they are all blocked")
                            break
                    self.context.log("main loop: putting back {0} blocked jobs".format(len(put_me_back)))
                    for j in put_me_back:
                        self.context.submit_job(j)
                except SyncQueue.Empty:
                    #uncommenting the following is seriously spammy
                    self.context.log("main loop: queue empty, sleeping for {0}s".format(POLLING_CYCLE))
                    time.sleep(POLLING_CYCLE)
                    next
                #exclude non-runnable jobs from the queue
                if job.function != None and hasattr(job.function, '__call__'):
                    self.context.log("main loop: running a job in a thread")
                    self.context.run_job(job)
                else:
                    self.context.log("main loop: dropping non-runnable job")

            else:
                self.context.log("main loop: no jobs to submit")
                time.sleep(POLLING_CYCLE)
        self.context.log("main cycle terminated")

class Scheduler():
    
    def __init__(self,threads = MIN_THREADS, debug = False):
        self._job_queue = SyncQueue.Queue()
        self._idle_threads = SyncQueue.Queue()
        self._threads_to_collect = SyncQueue.Queue()
        self._slots = 0
        self.adjust_slot_count(threads)
        self._debug = debug
        self._main_thread = None

    def log(self,x):
        if self._debug:
            print("wasync: " + x)
            sys.stdout.flush()

    def go(self, debug = None):
        if debug is not None:
            self._debug = debug
        self.log("started ThreadPoolExecutor")
        self._running = True
        self.log("submitting infinite loop")
        self.loop = MainLoop(self)
        self.loop.daemon = True
        self.loop.start()
        self.thread_collector = ThreadCollection(self,self._threads_to_collect)
        self.thread_collector.daemon = True
        self.thread_collector.start()
        return self.loop

    def shutdown(self):
        self.log("scheduler: shutting down")
        self._running = False
        self.loop.join()
        self.thread_collector.running = False
        self.thread_collector.join()

    def submit_job(self,job):
        self._job_queue.put(job)
        self.log("scheduler: job submitted")

    def after_job(self,result,job):
        try:
            job.determine(result)
        except Exception as e:
            print "Exception " + str(e) + " in module " + str(job.function.__module__) + ", function " + job.function.__name__
            print str(job.function.__code__.co_filename) + ", line "  + str(job.function.__code__.co_firstlineno)
            raise

    def adjust_slot_count(self,slot_count):
        #TODO: replace slots with semaphore
        slots_to_add = slot_count - self._slots
        if slots_to_add > 0:
            for i in range(1,slots_to_add):
                nt = Worker(self)
                nt.daemon = True
                nt.start()
                self._idle_threads.put(nt)
        if slots_to_add < 0:
            for i in range(1,-slots_to_add):
                ot = self._idle_threads.get(True)
                ot.running = False
                self._threads_to_collect.put(ot)
        self._slots = slot_count

    def _check_thread_pool_size(self):
        if self._idle_threads.qsize() < self._slots / 3:
            self.adjust_slot_count(min(self._slots * 2, MAX_THREADS))
        if self._idle_threads.qsize() > 3 * self._slots / 4:
            self.adjust_slot_count(max(self._slots / 2, MIN_THREADS))

    def worker_function(self,job):
        result = job.function()
        self.after_job(result,job)
#        self.log("job completed: {0}".format(job))
        [callback(result) for callback in job.callbacks]

    def run_job(self,job):
        self.log("run_job: checking pool size")
        self._check_thread_pool_size()
        self.log("run_job: {0} slots available".format(self._slots))
        t = self._idle_threads.get(True)
        t.wq.put(lambda: self.worker_function(job))

    def can_run_job(self):
        return self._slots > 0 and not self._job_queue.empty()
