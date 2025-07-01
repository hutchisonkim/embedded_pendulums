import argparse
from pyspark.sql import SparkSession
from spark.base_gen import BaseGenerator
from spark.utils import format_task_args
import json

class StateGen(BaseGenerator):

    def run(self, experiment_yaml):
        """
        Generate the initial state JSON based on the experiment YAML configuration.
        
        :param experiment_yaml: Parsed YAML configuration defining the experiment and state.
        :return: JSON object representing the initial state.
        """
        # Extract state variables and their default values
        state_variables = experiment_yaml.get("state", {}).get("list", [])
        initial_state = {}

        for variable in state_variables:
            var_name = variable["name"]
            default_value = variable.get("default", None)  # Use None if default is not defined
            initial_state[var_name] = default_value

        # Convert the state to JSON
        state_json = json.dumps({"state": initial_state}, indent=4)

        return state_json


class StateGenApp:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("ActionGenApp") \
            .getOrCreate()
        self.generator = StateGen(self.spark)

    def run(self, args):

        # Extract
        experiment_yaml = self.generator.read_yaml(args.experiment_yaml_filepath)

        # Transform
        state_json = self.generator.run(experiment_yaml)

        # Load
        self.generator.write_json(state_json, args.out_state_json_filepath)


def main():

    parser = argparse.ArgumentParser()
    
    parser.add_argument("--experiment_branch",
                        type=str)
    parser.add_argument("--bellman_branch",
                        type=str)
    parser.add_argument("--experiment_yaml_filepath",
                        type=str,
                        default="/data/generated/{experiment_branch}/experiment.yaml")
    parser.add_argument("--out_state_json_filepath",
                        type=str,
                        default="/data/generated/{bellman_branch}/state.json")
    
    args = parser.parse_args()
    args = format_task_args(args)

    app = StateGenApp()
    app.run(args)

if __name__ == "__main":
    main()
