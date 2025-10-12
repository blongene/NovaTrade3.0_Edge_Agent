import time
from functools import wraps
from gspread.exceptions import APIError

def retry_gspread(max_retries=3, delay=2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except APIError as e:
                    last_exception = e
                    print(f"⚠️ GSpread retry {attempt+1}/{max_retries} failed: {e}")
                    time.sleep(delay)
            raise last_exception
        return wrapper
    return decorator