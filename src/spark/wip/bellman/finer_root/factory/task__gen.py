import argparse
from spark.task_gen import TaskGenApp
from spark.utils import format_task_args, get_uuid_hash

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--base_task_path",
                        type=str,
                        default="/src/spark/")
    
    parser.add_argument("--tasks",
                        nargs="+",
                        default=[
                            "--experiment_branch={experiment_branch}",
                            "--bellman_branch={bellman_branch}",
                            "bellman/finer_root/initial/state",
                            
                            "--next_bellman_branch={next_bellman_branch}",
                            "bellman/finer_root/recursive/actions_proposal",

                            "--data_branch={data_branch}",
                            "--bellman_branch={next_bellman_branch}",
                            "--previous_bellman_branch={bellman_branch}",
                            "--factory_branch={next_factory_branch}",
                            "bellman/finer_root/recursive/task",
                            "task_dag",
                            "task_dag_run",
                            ]
                        )
    
    parser.add_argument("--factory_branch",
                        type=str)
    parser.add_argument("--data_branch",
                        type=str)
    parser.add_argument("--experiment_branch",
                        type=str)
    parser.add_argument("--bellman_branch",
                        type=str)
    parser.add_argument("--next_iteration_guid",
                        type=str,
                        default=f"{get_uuid_hash()}")
    parser.add_argument("--next_bellman_branch",
                        type=str,
                        default="bellman/{next_iteration_guid}")
    parser.add_argument("--next_factory_branch",
                        type=str,
                        default="factory/{next_iteration_guid}")
    parser.add_argument("--out_tasks_csv_filepath",
                        type=str,
                        default="/data/generated/{factory_branch}/tasks.csv")
    parser.add_argument("--out_task_dependencies_csv_filepath",
                        type=str,
                        default="/data/generated/{factory_branch}/task_dependencies.csv")
    
    args = parser.parse_args()
    args = format_task_args(args)
    
    app = TaskGenApp()
    app.run(args)

if __name__ == "__main":
    main()
