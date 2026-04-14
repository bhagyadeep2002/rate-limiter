import redis
from RedisTokenBucket import RedisTokenBucket
from fastapi import FastAPI, Request
redis_client = redis.Redis(host="localhost", port=6379, db=0)
token_bucket = RedisTokenBucket(redis_client=redis_client, capacity=10, refill_rate=1)

app = FastAPI()

@app.get("/")
def home():
    return {"message": "Welcome to the rate-limited API!"}

@app.get("/get-ip")
def get_ip(request: Request):
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        client_ip = forwarded.split(",")[0]
    client_ip = request.client.host
    if token_bucket.allow_request(user_id=client_ip)["allowed"]:
        return {"message": f"Your IP {client_ip} is allowed to access the resource."}
    else:
        return {"message": f"Rate limit exceeded for IP {client_ip}. Please try again later."}, 429