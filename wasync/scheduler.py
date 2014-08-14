# $Id: scheduler.py 34456 2014-04-08 18:27:00Z eda $
import time
import concurrent.futures
import Queue as SyncQueue

#we absolutely want all instances of wasync.Scheduler to talk to the same concurrent.futures
MIN_THREADS = 20
MAX_THREADS = 2048
POLLING_CYCLE = 0.01

class Scheduler():
    def __init__(self,threads = MIN_THREADS):
        self.update_threads(threads)
        self._job_queue = SyncQueue.Queue()

    def update_threads(self,threads):
        self._slots = threads

    def go(self):
        self._futures_scheduler = concurrent.futures.ThreadPoolExecutor(max_workers=self._slots)
        self._running = True
        return self._futures_scheduler.submit(self.loop)

    def shutdown(self):
        self._running = False
        self._futures_scheduler.shutdown()
        self._futures_scheduler = None

    def submit_job(self,job):
        self._job_queue.put(job)

    def after_job(self,result,job):
        self._slots += 1
        try:
            job.determine(result.result())
        except Exception as e:
            print "Exception " + str(e) + " in module " + str(job.function.__module__) + ", function " + job.function.__name__
            print str(job.function.__code__.co_filename) + ", line "  + str(job.function.__code__.co_firstlineno)
            raise

    def adjust_slot_count(self,slot_count):
        self._slots = slot_count
        self._futures_scheduler._max_workers = slot_count
        self._futures_scheduler._adjust_thread_count()

    def _check_thread_pool_size(self):
        if len(self._futures_scheduler._threads) < self._slots / 3:
            self.adjust_slot_count(max(self._slots / 2, MIN_THREADS))
        if len(self._futures_scheduler._threads) > 3 * self._slots / 4:
            self.adjust_slot_count(min(self._slots * 2, MAX_THREADS))

    def run_job(self,job):
        self._check_thread_pool_size()
        self._slots -= 1
        future = self._futures_scheduler.submit(job.function)
        future.add_done_callback(lambda result, job=job: self.after_job(result,job))
        [future.add_done_callback(f) for f in job.callbacks]

    def can_run_job(self):
        return self._slots > 0 and not self._job_queue.empty()

    def loop(self):
        while self._running:
            if self.can_run_job():
                try:
                    job = self._job_queue.get(True,POLLING_CYCLE)
                except Empty:
                    next
                #exclude non-runnable jobs from the queue
                if job.function != None and hasattr(job.function, '__call__'):
                    self.run_job(job)
            else:
                time.sleep(POLLING_CYCLE)
