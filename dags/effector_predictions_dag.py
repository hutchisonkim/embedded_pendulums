from airflow import DAG
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from utils import create_python_task, create_sensor_task, create_sensor_reset_task

# Default Arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 5),
    'retries': 1,
}

# Callable Functions Map
ASSETS = [
    "motion_proposal",
    "motion",
    "animation_proposal",
    "animation",
    "animation_preview",
    "spline",
    "spline_preview",
    "bisector_preview",
    "curvature_preview",
    "aggregate_preview",
]

TASKS = {
    "code_sensors": "{asset}_gen_sensor",
    "code_tests": "{asset}_gen_test",
    "data_generators": "{asset}_gen",
    "data_validators": "{asset}_validator",
}

PATHS = {
    "code_sensors": "/src/spark/{task}.py",
    "code_tests": "/src/spark/test/{task}.py",
    "data_generators": "/src/spark/{task}.py",
    "data_validators": "/src/spark/validator/{task}.py",
}

# DAG Definition
with DAG(
    "effector_transformations_development_dag",
    default_args=default_args,
    description="Development DAG for Effector Transformations",
    schedule_interval="@once",
    catchup=False
) as dag:

    # Data Generators
    data_preview_assets = []
    data_assets = []
    for asset in ASSETS:
        if "preview" in asset:
            data_preview_assets.append(asset)
        else:
            data_assets.append(asset)

    data_generators = {
        asset: create_python_task(
            task=TASKS["data_generators"].format(asset=asset),
            path=PATHS["data_generators"].format(task=TASKS["data_generators"].format(asset=asset)),
        )
        for asset in data_assets
    }

    # Data
    with TaskGroup("preview_generators", tooltip="Preview Generators") as code_tests:
        data_preview_generators = {
            asset: create_python_task(
                task=TASKS["data_generators"].format(asset=asset),
                path=PATHS["data_generators"].format(task=TASKS["data_generators"].format(asset=asset)),
            )
            for asset in data_preview_assets
        }

    #join normal generators with preview generators
    generators = {**data_generators, **data_preview_generators}
    

    # Data Validators
    with TaskGroup("data_validators", tooltip="Data Validators") as data_validators:
        validators = {
            asset: create_python_task(
                task=TASKS["data_validators"].format(asset=asset),
                path=PATHS["data_validators"].format(task=TASKS["data_validators"].format(asset=asset)),
            )
            for asset in ASSETS
        }

    # Code Sensors
    with TaskGroup("code_sensors", tooltip="Code Sensors") as code_sensors:
        sensors = {
            asset: create_sensor_task(
                task=TASKS["code_sensors"].format(asset=asset),
                path=PATHS["code_sensors"].format(task=TASKS["data_generators"].format(asset=asset)),
                op_kwargs={
                    "tasks_to_clear": [f"""code_tests.{TASKS["code_tests"].format(asset=asset)}"""],
                    # "tasks_to_clear": ["code_sensors.code_sensors_reset", f"""code_tests.{TASKS["code_tests"].format(asset=asset)}"""],
                    "clear_downstream": True,
                },
            )
            for asset in ASSETS
        }

    # Code Testers
    with TaskGroup("code_tests", tooltip="Code Tests") as code_tests:
        tests = {
            asset: create_python_task(
                task=TASKS["code_tests"].format(asset=asset),
                path=PATHS["code_tests"].format(task=TASKS["code_tests"].format(asset=asset)),
                op_kwargs={
                    "tasks_to_clear": [f"""code_sensors.{TASKS["code_sensors"].format(asset=asset)}"""],
                    "clear_downstream": False,
                },
            )
            for asset in ASSETS
        }

        # sensor_reset = create_sensor_reset_task(
        #     task="code_sensors_reset",
        #     op_kwargs={
        #         # "tasks_to_clear": [f"""code_sensors.{TASKS["code_sensors"].format(asset=asset)}""" for asset in ASSETS],
        #         # "clear_downstream": False,
        #     },
        # )

    # Task Dependencies
    generators["motion_proposal"] >> generators["motion"]
    generators["animation_proposal"] >> generators["animation"]
    generators["motion"] >> generators["animation_proposal"]
    generators["animation"] >> generators["animation_preview"]
    generators["animation"] >> generators["spline"]
    generators["spline"] >> generators["bisector_preview"]
    generators["spline"] >> generators["spline_preview"]
    generators["spline"] >> generators["curvature_preview"]
    generators["animation_preview"] >> generators["aggregate_preview"]
    generators["bisector_preview"] >> generators["aggregate_preview"]
    generators["spline_preview"] >> generators["aggregate_preview"]
    generators["curvature_preview"] >> generators["aggregate_preview"]


    for asset in ASSETS:
        sensors[asset] >> tests[asset] >> generators[asset] >> validators[asset]
        # sensors[asset] >> sensor_reset
