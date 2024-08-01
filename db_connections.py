import sqlite3
from sqlalchemy import create_engine
import time
from functools import wraps
from logger_setup import logger


def retry(max_attempts=3, delay=1):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts += 1
                    logger.warning("Attempt %d failed: %s", attempts, str(e))
                    if attempts == max_attempts:
                        logger.error("All %d attempts failed.", max_attempts)
                        raise
                    time.sleep(delay)
        return wrapper
    return decorator


@retry(max_attempts=3, delay=2)
def connect_sqlite(db_file):
    try:
        return sqlite3.connect(db_file)
    except sqlite3.Error as e:
        logger.error("Error connecting to SQLite database: %s", e)
        return None


@retry(max_attempts=3, delay=2)
def connect_mysql(host, user, password, database):
    try:
        engine = create_engine(
            f"mysql+pymysql://{user}:{password}@{host}/{database}?charset=utf8mb4")
        return engine
    except Exception as e:
        logger.error("Error connecting to MySQL database: %s", e)
        return None
