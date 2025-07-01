import argparse
from pyspark.sql import SparkSession
from spark.base_gen import BaseGenerator
from spark.utils import format_task_args

class ActionGen(BaseGenerator):

    def run(self, action_index, experiment_yaml, actions_proposal_json):

        action_priorities = actions_proposal_json.get("action_priorities", [])

        if action_index < 0 or action_index >= len(action_priorities):
            raise IndexError("Index out of bounds for action priorities.")
        
        action_id = action_priorities[action_index]["action_id"]
        action = next((a for a in experiment_yaml.get("actions", {}).get("list", []) if a["id"] == action_id), None)
        if not action:
            raise ValueError(f"Action with id {action_id} not found in experiment YAML.")
        
        task = action.get("task", "")

        action_json = {
            "action_index": action_index,
            "action_task": task,
        }

        return action_json


class ActionGenApp:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("ActionGenApp") \
            .getOrCreate()
        self.generator = ActionGen(self.spark)

    def run(self, args):

        # Extract
        action_index = args.action_index
        experiment_yaml = self.generator.read_yaml(args.experiment_yaml_filepath)
        actions_proposal_json = self.generator.read_json(args.actions_proposal_json_filepath)

        # Transform
        action_json = self.generator.run(action_index, experiment_yaml, actions_proposal_json)

        # Load
        self.generator.write_json(action_json, args.out_action_json_filepath)


def main():

    parser = argparse.ArgumentParser()
    
    parser.add_argument("--action_index",
                        type=int)
    parser.add_argument("--experiment_branch",
                        type=str)
    parser.add_argument("--bellman_branch",
                        type=str)
    parser.add_argument("--experiment_yaml_filepath",
                        default="/data/generated/{experiment_branch}/experiment.yaml",
                        type=str)
    parser.add_argument("--actions_proposal_json_filepath",
                        default="/data/generated/{bellman_branch}/actions_proposal.json",
                        type=str)
    parser.add_argument("--out_action_json_filepath",
                        default="/data/generated/{bellman_branch}/action.json",
                        type=str)
    
    args = parser.parse_args()
    args = format_task_args(args)

    app = ActionGenApp()
    app.run(args)

if __name__ == "__main":
    main()
