import json
import argparse
from pyspark.sql import SparkSession
from spark.base_gen import BaseGenerator
from spark.utils import format_task_args


class ActionsProposalGen(BaseGenerator):
    def run(self, experiment_yaml, state_json):
        """
        Generate a JSON with action priorities based on the experiment YAML and the current state JSON.
        
        :param experiment_yaml: Parsed YAML configuration defining actions, policies and rewards.
        :param state_json: JSON containing the current state.
        :return: JSON with actions and their respective priorities.
        """
        # Load actions and policies from the YAML configuration
        actions = experiment_yaml.get("actions", {}).get("list", [])
        policies = experiment_yaml.get("policies", {}).get("list", [])
        state_variables = state_json.get("state", {})

        # Initialize result
        action_priorities = []

        priority_total = 0
        for action in actions:
            action_id = action["id"]
            preconditions = action.get("conditions", [])
            rewards = action.get("rewards", [])
            priority = 0

            # Evaluate action preconditions
            if all(self._evaluate_condition(cond, state_variables) for cond in preconditions):
                for reward in rewards:
                    formula = reward["formula"]
                    priority += self._evaluate_formula(formula, state_variables)

            # Apply policy to determine final priority
            for policy in policies:
                if policy["method"] == "epsilon_greedy":
                    priority = self._apply_epsilon_greedy(priority, policy)
                elif policy["method"] == "max_reward":
                    priority = self._apply_max_reward(priority, policy)

            action_priorities.append({"action_id": action_id, "priority": int(priority)})
            
            priority_total += int(priority)

        # Sort actions by priority in descending order
        action_priorities.sort(key=lambda x: x["priority"], reverse=True)

        # Prepare the final actions proposal
        actions_proposal = {
            "action_priorities": action_priorities,
            "total_priority": priority_total
        }

        # Convert result to JSON
        return json.dumps(actions_proposal, indent=4)

    def _evaluate_condition(self, condition, state_variables):
        """
        Evaluate a condition string against the current state variables.
        """
        try:
            condition_eval = condition
            for key, value in state_variables.items():
                condition_eval = condition_eval.replace(f"state.{key}", str(value))
            return eval(condition_eval)
        except Exception as e:
            raise ValueError(f"Error evaluating condition '{condition}': {e}")

    def _evaluate_formula(self, formula, state_variables):
        """
        Evaluate a reward formula string against the current state variables.
        """
        try:
            formula_eval = formula
            for key, value in state_variables.items():
                formula_eval = formula_eval.replace(f"state.{key}", str(value))
            return eval(formula_eval)
        except Exception as e:
            raise ValueError(f"Error evaluating formula '{formula}': {e}")

    def _apply_epsilon_greedy(self, priority, policy):
        """
        Apply epsilon-greedy policy to adjust priority.
        """
        epsilon = policy["parameters"].get("epsilon", 0.2)
        return priority * (1 - epsilon)

    def _apply_max_reward(self, priority, policy):
        """
        Apply max-reward policy to adjust priority.
        """
        decay = policy["parameters"].get("decay", 0.95)
        return priority * decay


class ActionsProposalGenApp:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("ActionsProposalGenApp") \
            .getOrCreate()
        self.generator = ActionsProposalGen(self.spark)

    def run(self, args):
        # Extract
        experiment_yaml = self.generator.read_yaml(args.experiment_yaml_filepath)
        state_json = self.generator.read_json(args.state_json_filepath)

        # Transform
        actions_proposal_json = self.generator.run(experiment_yaml, state_json)

        # Load
        self.generator.write_json(actions_proposal_json, args.out_actions_proposal_json_filepath)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--experiment_branch",
                        type=str)
    parser.add_argument("--bellman_branch",
                        type=str)
    parser.add_argument("--next_bellman_branch",
                        type=str)
    parser.add_argument("--experiment_yaml_filepath",
                        type=str,
                        default="/data/generated/{experiment_branch}/experiment.yaml")
    parser.add_argument("--state_json_filepath",
                        type=str,
                        default="/data/generated/{bellman_branch}/state.json")
    parser.add_argument("--out_actions_proposal_json_filepath",
                        type=str,
                        default="/data/generated/{next_bellman_branch}/actions_proposal.json")

    args = parser.parse_args()
    args = format_task_args(args)

    app = ActionsProposalGenApp()
    app.run(args)


if __name__ == "__main__":
    main()
