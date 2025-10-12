from datetime import datetime

def log_local(message, file='local_log.txt'):
    try:
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        with open(file, 'a') as f:
            f.write(f"[{now}] {message}\n")
    except Exception as e:
        print(f"❌ Local log write failed: {e}")