import pendulum
from airflow.decorators import dag, task
import logging
from functools import wraps
import time
import numpy as np

def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time.time()
        result = f(*args, **kw)
        te = time.time()
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
    
@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False)

def function():
    task_params = {
        'function0': (100,5),
        'function1': (100,5),
        'function2': (100,5),
        'function3': (100,5),
        'function4': (100,5),
        'function5': (100,5),
        'function6': (100,5),
        'function7': (100,5),
        'function8': (100,5),
        'function9': (100,5),
        'function10': (100,5),
        'function11': (100,5),
        'function12': (100,5),
        'function13': (100,5),
        'function14': (100,5),
        'function15': (100,5),
        'function16': (100,5),
        'function17': (100,5),
        'function18': (100,5),
    }
    tasks = {}
    for task_id, params in task_params.items():
        tasks[task_id] = create_timed_task(task_id, *params)
    
    tasks['function0'] >>tasks['function1'] >>tasks['function2'] >>tasks['function3'] >>tasks['function4'] 

    tasks['function0'] >>tasks['function5'] >>tasks['function6'] >>tasks['function7'] >>tasks['function8'] 

    tasks['function0'] >>tasks['function9'] >>tasks['function10'] >>tasks['function11'] >>tasks['function12'] 

    tasks['function0'] >>tasks['function13'] >>tasks['function14'] >>tasks['function15'] >>tasks['function16'] 

    tasks['function6'] >>tasks['function17'] >>tasks['function18'] 

etl_dag=function()