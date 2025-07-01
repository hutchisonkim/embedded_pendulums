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
                            "--action_index={action_index}",
                            "--experiment_branch={experiment_branch}",
                            "--bellman_branch={bellman_branch}",
                            "bellman/finer_root/recursive/action",

                            "--previous_bellman_branch={previous_bellman_branch}",
                            "--factory_branch={state_factory_branch}",
                            "bellman/finer_root/recursive/state/factory/task",
                            "task_dag",
                            "task_dag_run",

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
    
    parser.add_argument("--action_index",
                        default="0",
                        type=str)
    parser.add_argument("--factory_branch",
                        type=str)
    parser.add_argument("--state_factory_branch",
                        default="{factory_branch}/state",
                        type=str)
    parser.add_argument("--data_branch",
                        type=str)
    parser.add_argument("--experiment_branch",
                        type=str)
    parser.add_argument("--previous_bellman_branch",
                        type=str)
    parser.add_argument("--bellman_branch",
                        type=str)
    parser.add_argument("--next_iteration_guid",
                        default=f"{get_uuid_hash()}",
                        type=str)
    parser.add_argument("--next_bellman_branch",
                        default="bellman/{next_iteration_guid}",
                        type=str)
    parser.add_argument("--next_factory_branch",
                        default="factory/{next_iteration_guid}",
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

#TODO: make a test run to ensure that the effector_predictions graph is generated correctly
#TODO: fix task__gen, task_dag__gen, and task_dag_run__gen to use the new task format
#TODO: run the whole graph including the bellman framework
#TODO: create preview tasks to generate images and videos of the results
#TODO: export graph to a flat airflow format (connections rather than sub-dags)
#TODO: export graph to a generic format for visualization (graphviz) and hardware acceleration (kubeflow, gke, cuda)

"""
idea:

- start with sub-dags
- then instead of a sub-dag, stack the ancestors of the task to create a single sequence
-- new dags show past tasks are already successful
-- each head recreates the dag, meaning that there is as many dags as there are heads
-- then we can merge the heads into a single graph
-- then we can reduce the graph by removing writer/reader tasks
-- then we can further reduce the graph 
--- using other manually authored mergers
--- using machine learning to predict alternative graphs 

"""
