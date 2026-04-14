import time
import redis

class RedisTokenBucket:
    def __init__(self, redis_client, capacity, refill_rate):
        self.refill_rate = refill_rate
        self.capacity = capacity
        self.redis = redis_client

    def allow_request(self, user_id, retries=5):
        key = f"rate_limit:{user_id}"
        for _ in range(retries):
            try:
                with self.redis.pipeline() as pipe:
                    pipe.watch(key)
                    data = pipe.hgetall(key)
                    now = time.time()
                    if data:
                        tokens = float(data.get(b"tokens", 0))
                        last_refill = float(data.get(b"last_refill", now))
                    else:
                        tokens = self.capacity
                        last_refill = now
                    elapsed = now - last_refill
                    tokens = min(self.capacity, tokens + elapsed * self.refill_rate)
                    allowed = tokens >= 1
                    if allowed:
                        tokens -= 1
                    pipe.multi()
                    pipe.hset(key, mapping={
                        "tokens": tokens,
                        "last_refill": now
                    })
                    pipe.expire(key, 3600)
                    pipe.execute()
                    return {
                        "allowed": allowed,
                        "tokens": tokens,
                        "last_refill": last_refill
                    }
            except redis.WatchError:
                continue
        return {
                "allowed": allowed,
                "tokens": tokens,
                "last_refill": last_refill
            }
    
    def borrow_requests(self, user_id, num_requests, retries=5):
        key = f"rate_limit:{user_id}"
        for _ in range(retries):
            try:
                with self.redis.pipeline() as pipe:
                    pipe.watch(key)
                    data = pipe.hgetall(key)
                    now = time.time()
                    if data:
                        tokens = float(data.get(b"tokens", 0))
                        last_refill = float(data.get(b"last_refill", now))
                    else:
                        tokens = self.capacity
                        last_refill = now
                    elapsed = now - last_refill
                    tokens = min(self.capacity, tokens + elapsed * self.refill_rate)
                    tokens_allowed = min(tokens, num_requests)
                    tokens -= tokens_allowed
                    pipe.multi()
                    pipe.hset(key, mapping={
                        tokens_allowed: tokens,
                        "last_refill": now
                    })
                    pipe.expire(key, 3600)
                    pipe.execute()
                    return {
                        "tokens_allowed": tokens_allowed,
                        "tokens_remaining": tokens,
                        "last_refill": last_refill
                    }
            except redis.WatchError:
                continue
                    