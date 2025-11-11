
import os
import multiprocessing

# Performance optimizations for distributed upload system
# Multiple workers safe due to atomic job claiming via PostgreSQL

# Calculate workers based on CPU cores (safe for 2-vCPU autoscale or larger VMs)
def calculate_workers():
    """
    Calculate optimal worker count for the environment:
    - 2 vCPU: 4 workers
    - 4 vCPU: 8 workers
    - 6+ vCPU: 8 workers (capped)
    Formula: min(8, max(2, cpu_count * 2))
    """
    cpu_count = multiprocessing.cpu_count()
    optimal = cpu_count * 2
    # Cap at 8 workers max, minimum 2 workers
    return max(2, min(8, optimal))

workers = int(os.environ.get('WEB_CONCURRENCY', str(calculate_workers())))
worker_class = "sync"
worker_connections = 1000
max_requests = 500  # Restart workers more frequently to prevent memory leaks
max_requests_jitter = 50  # Add jitter to prevent all workers restarting simultaneously
preload_app = False  # Disabled to prevent build timeouts
timeout = 300  # 5min timeout for large multi-site file processing
keepalive = 5

# Memory management
worker_tmp_dir = "/dev/shm"

# Binding
bind = "0.0.0.0:5000"

# Logging
loglevel = "info"
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" %(D)s'

# Process naming
proc_name = "stat_manager"

# Graceful timeout
graceful_timeout = 60

def when_ready(server):
    """Called once when the master process is ready"""
    server.log.info("Stat Manager ready to serve requests")
    # Background worker will be started by first worker via init_app()

def worker_int(worker):
    worker.log.info("Worker received INT or QUIT signal")

def pre_fork(server, worker):
    server.log.info("Worker spawned (pid: %s)", worker.pid)
