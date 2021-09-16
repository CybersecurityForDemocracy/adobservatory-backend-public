import multiprocessing

# Use 1 worker process with multiple threads. This way workers threads have access to same cache (caching
# library keeps cache in process memory)
workers = 1
threads = multiprocessing.cpu_count() * 4 + 1
worker_class = 'gthread'
# Allow request workers to run for up to 15 min
timeout = 900
