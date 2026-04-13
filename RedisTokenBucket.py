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
                    return allowed
            except redis.WatchError:
                continue
        return False
    