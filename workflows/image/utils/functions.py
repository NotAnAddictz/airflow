import time
from airflow.operators.python import PythonOperator
import numpy as np
import logging
from airflow.decorators import task
from functools import wraps
def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        logging.info('func:%r args:[%r, %r] took: %f sec. Start: %f, End: %f' % (f.__name__, args, kw, te-ts, ts, te))
        return result
    return wrap
def simulateRuntime(memory_mb,runtime_sec):
    """
    Simulate memory consumption and runtime.

    :param memory_mb: Amount of memory to consume in MB.
    :param runtime_sec: Duration to sleep in seconds.
    """
    # Simulate memory consumption
    data = np.zeros((memory_mb * 1024 * 1024) // 8)  # Approximate allocation
    
    # Simulate runtime
    time.sleep(runtime_sec)


def create_timed_task(task_id, param1, param2):
    """
    Factory function to create a timed Airflow task.

    Parameters:
    - task_id (str): Unique identifier for the task.
    - param1 (int): First parameter for simulate_task.
    - param2 (int): Second parameter for simulate_task.

    Returns:
    - Airflow Task object
    """
    @task(task_id=task_id)
    @timing
    def task_func():
        return simulateRuntime(param1, param2)
    
    return task_func()