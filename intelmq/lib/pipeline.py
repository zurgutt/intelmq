import redis
import time

class Pipeline():
    def __init__(self, host="127.0.0.1", port="6379", db=2):
        self.host = host
        self.port = port
        self.db = db
        
        self.redis = redis.Redis(
                                 host = self.host,
                                 port = int(self.port),
                                 db = self.db,
                                 socket_timeout = 50000
                                )

    def source_queues(self, source_queue):
        self.source_queue = source_queue
        if source_queue:
            self.internal_queue = source_queue + "-internal"
            
    def destination_queues(self, destination_queues):
        if destination_queues and type(destination_queues) is not list:
            destination_queues = destination_queues.split()     
        self.destination_queues = destination_queues

    
    def disconnect(self):
        pass
    
    def sleep(self, interval):
        time.sleep(interval)

    def send(self, message):
        for destination_queue in self.destination_queues:
            self.redis.lpush(destination_queue, message)

    def receive(self):
        if self.redis.llen(self.internal_queue) > 0:
            return self.redis.lindex(self.internal_queue, -1)
        return self.redis.brpoplpush(self.source_queue, self.internal_queue, 0)
        
    def acknowledge(self):
        return self.redis.rpop(self.internal_queue)

    def count_queued_messages(self, queues):
        qdict = dict()
        for queue in queues:
            qdict[queue] = self.redis.llen(queue)
        return qdict
    
    def reserve(self, count=1, blocking=True):
        pull_count = count - self.redis.llen(self.internal_queue)
        
        if pull_count > 0:
            for i in xrange(pull_count):
                if blocking:
                    self.redis.brpoplpush(self.source_queue,
                                          self.internal_queue, 0)
                else:
                    self.redis.rpoplpush(self.source_queue,
                                         self.internal_queue)
                    
        return self.redis.lrange(self.internal_queue,
                                 0, self.redis.llen(self.internal_queue))

    def release(self, events):
        for message in events:
            for destination_queue in self.destination_queues:
                self.redis.lpush(destination_queue, message)
        
        # If item count in memory matches that of in redis, assume all have
        # been released to output. Otherwise, match and release.
        if events.count() == self.redis.llen(self.internal_queue):
            self.redis.delete(self.internal_queue)
        else:
            # TODO: Should match by some id here and delete selectively.
            self.redis.delete(self.internal_queue)
            
 
# Algorithm
# ---------
# [Receive]     B RPOP LPUSH   source_queue ->  internal_queue
# [Send]        LPUSH          message      ->  destination_queue
# [Acknowledge] RPOP           message      <-  internal_queue
