from rq import Worker

class KmWorker(Worker):
    def __init__(self, queues=None, *args, **kwargs):
        super().__init__(queues, *args, **kwargs)
        
        