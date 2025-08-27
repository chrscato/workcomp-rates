# Gunicorn configuration file
import multiprocessing

# Server socket
bind = "127.0.0.1:8000"
backlog = 2048

# Worker processes
workers = multiprocessing.cpu_count() * 2 + 1
worker_class = "sync"
worker_connections = 1000
timeout = 30
keepalive = 2

# Restart workers after this many requests, to help prevent memory leaks
max_requests = 1000
max_requests_jitter = 50

# Logging
accesslog = "/var/log/workcomp-rates/gunicorn_access.log"
errorlog = "/var/log/workcomp-rates/gunicorn_error.log"
loglevel = "info"

# Process naming
proc_name = "workcomp-rates"

# User/group
user = "www-data"
group = "www-data"

# Preload app for better performance
preload_app = True