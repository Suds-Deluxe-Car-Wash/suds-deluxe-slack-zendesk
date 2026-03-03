"""Gunicorn configuration for Render deployment.

Single worker prevents multiple connection pools competing for
Render's limited PostgreSQL connection slots.  preload_app=False
ensures the psycopg_pool ConnectionPool (and its internal management
threads) is created inside the worker process, not the master.
"""
import os

workers = 1
threads = int(os.getenv("GUNICORN_THREADS", "4"))
worker_class = "gthread"
bind = f"0.0.0.0:{os.getenv('PORT', '3000')}"

timeout = int(os.getenv("GUNICORN_TIMEOUT", "30"))
graceful_timeout = 10

preload_app = False

accesslog = "-"
loglevel = os.getenv("GUNICORN_LOG_LEVEL", "info")
