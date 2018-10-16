import queue


class Queue(object):
    def __init__(self, timeout=5, maxsize=0):
        self.url_queue = queue.LifoQueue(maxsize)
        self.timeout = timeout

    def is_empty(self):
        return self.url_queue.empty()

    def put(self, item):
        self.url_queue.put(item)

    def get(self):
        try:
            return self.url_queue.get(timeout=self.timeout)
        except Exception as e:
            print(e)
            return None

    def size(self):
        return self.url_queue.qsize()
