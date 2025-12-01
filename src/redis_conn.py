from redis import Redis
import time

try:
    import src.global_vars as gvars
except Exception:
    import global_vars as gvars

redis_conn: Redis = None




def wait_for_redis_startup(retries: int = 5) -> None:
    """Waits for Redis to start up"""
    global redis_conn
    if redis_conn is not None:
        return

    for _ in range(retries):
        try:
            redis_conn = Redis(host=gvars.redis_host, port=gvars.redis_port)
            redis_conn.ping()
            return
        except Exception as e:
            print(f"Waiting for Redis to start up... ({e})")
            time.sleep(1)

    raise Exception(f"Could not connect to Redis after {retries} retries")

# on importing this module, wait for Redis to start up
wait_for_redis_startup()