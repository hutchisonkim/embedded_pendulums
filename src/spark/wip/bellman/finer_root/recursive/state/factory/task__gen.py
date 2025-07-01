import argparse
from spark.task_gen import TaskGenApp
from spark.utils import format_task_args

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--base_task_path",
                        type=str,
                        default="/src/spark/")
    
    parser.add_argument("--tasks",
                        nargs="+",
                        default=[

                            "--data_branch={data_branch}",
                            "--bellman_branch={bellman_branch}",
                            "--factory_branch={factory_branch}/action/factory",
                            "ACTION_TASK",
                            "task_dag",
                            "task_dag_run",
                            # "ACTION_TASK" is the bellman action to be executed for this iteration
                            # specifies the data branch as the context for the action


                            "--experiment_branch={experiment_branch}",
                            "--bellman_branch={bellman_branch}",
                            "bellman/state",
                            # "bellman/state" is a generic state generator
                            # takes the data in a context and converts it to a state
                            # specifies the experiment YAML to map the data to the state variables
                            # specifies the data branch as the source for retrieving the data

                            ],
                        type=str)
    
    parser.add_argument("--action_index",
                        type=int)
    parser.add_argument("--experiment_branch",
                        type=str)
    parser.add_argument("--data_branch",
                        type=str)
    parser.add_argument("--bellman_branch",
                        type=str)
    parser.add_argument("--factory_branch",
                        type=str)
    parser.add_argument("--actions_proposal_json_filepath",
                        default="/data/generated/{bellman_branch}/actions_proposal.json",
                        type=str)
    parser.add_argument("--out_tasks_csv_filepath",
                        default="/data/generated/{factory_branch}/tasks.csv",
                        type=str)
    parser.add_argument("--out_task_dependencies_csv_filepath",
                        default="/data/generated/{factory_branch}/task_dependencies.csv",
                        type=str)
    
    args = parser.parse_args()
    args = format_task_args(args)
    
    app = TaskGenApp()
    app.run(args)

if __name__ == "__main":
    main()
