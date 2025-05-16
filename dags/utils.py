
from airflow.models import Variable
from airflow.utils.db import provide_session
from airflow.models import TaskInstance
import os
from time import sleep
from airflow.models.dagrun import DagRun
from airflow.utils.db import provide_session
from airflow.utils.timezone import parse as parse_datetime
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.python import PythonSensor




def create_python_task(task, path, op_kwargs=None):
    return PythonOperator(
        task_id=task,
        python_callable=lambda **k: run_script(path, **k),
        op_kwargs=op_kwargs,
    )

def create_sensor_task(task, path, op_kwargs):
    return PythonSensor(
        task_id=task,
        python_callable=lambda **k: wait_for_specific_file_changes(path, **k),
        op_kwargs=op_kwargs,
    )

def create_sensor_reset_task(task, op_kwargs):
    return PythonOperator(
        task_id=task,
        python_callable=lambda **k: clear_task_callable(**k),
        op_kwargs=op_kwargs,
    )

def run_script(file_path, **kwargs):
    try_clear_task(**kwargs)
    try:
        import sys
        base_path = "/src"
        if file_path not in sys.path:
            sys.path.append(base_path)
        module_path = file_path.replace(base_path + "/", "").replace("/", ".").replace(".py", "")
        print(f"file_path: {file_path}")
        print(f"module_path: {module_path}")
        module_name = module_path.split(".")[-1]
        print(f"module_name: {module_name}")
        # module_name = file_path.split("/")[-1].replace(".py", "")
        sys.argv = [f"{module_name}.py"]
        # module = __import__(f"spark.{module_name}", fromlist=["main"])
        module = __import__(module_path, fromlist=["main"])
        module.main()
    except Exception as e:
        print(f"Error in {file_path}: {e}")
        raise
    
    
def get_mtime(path):
    """Get the last modified time (mtime) of the file or folder."""
    return os.path.getmtime(path) if os.path.exists(path) else None


def wait_for_specific_file_changes(file_path, **kwargs):
    """
    Wait until the specified file is modified, but immediately succeed on the first poke of each DAG run.
    """
    sensor_task_id = kwargs['task'].task_id
    execution_date = kwargs['execution_date']  # Get the current DAG run's execution date
    unique_run_key = f"{sensor_task_id}_run_{execution_date}"

    # Check if this specific run has already succeeded
    already_succeeded = Variable.get(unique_run_key, default_var="false").lower() == "true"

    if already_succeeded:
        # This run has already succeeded, proceed with normal logic
        print(f"Run-specific key '{unique_run_key}' detected as succeeded for sensor '{sensor_task_id}'.")
    else:
        # Mark this specific run as successful and immediately return success
        Variable.set(unique_run_key, "true")
        print(f"First poke for run-specific key '{unique_run_key}' detected. Marking as success.")
        # try_clear_task(**kwargs)
        return True  # Immediately succeed on the first poke for this run

    # Normal sensor logic
    last_mtime = Variable.get(f"{file_path}_mtime", default_var="0")
    last_mtime = float(last_mtime)

    current_mtime = get_mtime(file_path)
    if current_mtime and current_mtime > last_mtime:
        Variable.set(f"{file_path}_mtime", str(current_mtime))
        try_clear_task(**kwargs)
        return True  # File has been modified

    sleep(5)  # Wait before checking again
    return False

def try_clear_task(**kwargs):
    if 'tasks_to_clear' in kwargs:
        clear_task_callable(**kwargs)
    
def clear_task_callable(**kwargs):
    """
    Clears multiple specific task instances within the same DAG run, but only if their current state is 'success' or 'failed'.
    """
    @provide_session
    def clear_tasks(session=None):
        # Extract dag_id and execution_date from kwargs
        dag_id = kwargs['dag'].dag_id
        execution_date = kwargs['execution_date']
        tasks_to_clear = kwargs['tasks_to_clear']  # Expecting a list of task IDs

        # Ensure execution_date is a datetime object
        if isinstance(execution_date, str):
            execution_date = parse_datetime(execution_date)

        # Get the current DagRun
        dagrun = session.query(DagRun).filter(
            DagRun.dag_id == dag_id,
            DagRun.execution_date == execution_date
        ).first()
        
        if not dagrun:
            raise ValueError(f"No DAG run found for DAG ID '{dag_id}' at '{execution_date}'.")

        # Iterate over the targeted task IDs and clear their states if their current state is 'success' or 'failed'
        for targeted_task_id in tasks_to_clear:
            task_instance = session.query(TaskInstance).filter(
                TaskInstance.dag_id == dag_id,
                TaskInstance.task_id == targeted_task_id,
                TaskInstance.execution_date == execution_date
            ).first()

            if not task_instance:
                raise ValueError(f"No TaskInstance found for task ID '{targeted_task_id}'.")

            # if not equal to running or none, clear the task instance
            if task_instance.state == 'success' or task_instance.state == 'failed':
                print(f"Clearing task '{targeted_task_id}' in DAG '{dag_id}' for execution_date '{execution_date}'...")
                task_instance.set_state(None, session)

                # Check if the downstream property exists in kwargs
                if 'clear_downstream' not in kwargs:
                    continue

                # Clear all downstream tasks if their current state is 'success'
                dag = kwargs['dag']
                downstream_task_ids = dag.get_task(targeted_task_id).get_flat_relatives(upstream=False)
                for downstream_task_id in downstream_task_ids:
                    downstream_task_instance = session.query(TaskInstance).filter(
                        TaskInstance.dag_id == dag_id,
                        TaskInstance.task_id == downstream_task_id.task_id,
                        TaskInstance.execution_date == execution_date
                    ).first()

                    if downstream_task_instance and (downstream_task_instance.state == 'success' or downstream_task_instance.state == 'failed'):
                        print(f"Clearing downstream task '{downstream_task_id.task_id}' in DAG '{dag_id}' for execution_date '{execution_date}'...")
                        downstream_task_instance.set_state(None, session)

        # Commit the session after processing all tasks
        session.commit()
        print(f"All targeted tasks cleared successfully for DAG '{dag_id}' and execution_date '{execution_date}'.")

    clear_tasks()
