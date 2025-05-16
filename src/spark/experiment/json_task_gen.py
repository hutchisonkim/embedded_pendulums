import os
import shutil
import json
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from spark.base_gen import BaseGenerator

class JsonTaskGenerator(BaseGenerator):

    def explode_dimensions(self, dimensions):
        from itertools import product
        keys, values = zip(*dimensions.items())
        combinations = [dict(zip(keys, v)) for v in product(*values)]
        return self.spark.createDataFrame(combinations)

    def apply_dependencies(self, df, dependencies):
        all_dependencies = []

        for dependency in dependencies:
            operations = dependency.get("operations", [])
            partition_by = None
            filters = []
            order_by = []

            for op in operations:
                if "partition" in op:
                    partition_by = op["partition"]
                elif "filter" in op:
                    filters.append(op["filter"])
                elif "order" in op:
                    order_by.append(op["order"])

            filtered_df = df
            for f in filters:
                for col, values in f.items():
                    filtered_df = filtered_df.filter(F.col(col).isin(values))

            if order_by:
                for col, values in order_by[0].items():
                    order_expr = F.when(F.lit(False), None)
                    for i, value in enumerate(values):
                        order_expr = F.when(F.col(col) == value, i).otherwise(order_expr)
                    filtered_df = filtered_df.withColumn(f"{col}_order_index", order_expr)

                if partition_by:
                    window = Window.partitionBy(*partition_by).orderBy(f"{col}_order_index")
                else:
                    window = Window.orderBy(f"{col}_order_index")

                filtered_df = filtered_df.withColumn("sequence_index", F.row_number().over(window))

            window_lag = Window.partitionBy(*partition_by).orderBy("sequence_index") if partition_by else Window.orderBy("sequence_index")
            dependencies_df = filtered_df.withColumn("source_task", F.col("task")) \
                                         .withColumn("target_task", F.lag("task").over(window_lag)) \
                                         .filter(F.col("target_task").isNotNull()) \
                                         .select("source_task", "target_task")

            all_dependencies.append(dependencies_df)

        combined_dependencies = all_dependencies[0]
        for dep in all_dependencies[1:]:
            combined_dependencies = combined_dependencies.union(dep)

        return combined_dependencies

    def resolve_paths(self, df, paths):
        for key, path_template in paths.items():
            conditions = key.split('&')
            for condition in conditions:
                column, value = condition.split('=')
                df = df.withColumn(
                    path_template.format(asset=F.col("asset"), stage=F.col("stage"))
                )
        return df

class JsonTaskApp:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("JsonTasks") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        self.generator = JsonTaskGenerator(self.spark)

    def run(self, args):
        # Extract
        task_proposals_json = self.generator.read_json(args.task_proposals_json_filepath)

        # Transform
        tasks = self.generator.explode_dimensions(task_proposals_json["dimensions"])
        sequences = self.generator.apply_dependencies(tasks, task_proposals_json["dependencies"])
        resolved = self.generator.resolve_paths(tasks, task_proposals_json["paths"])

        # Load
        self.generator.write_csv(resolved, args.out_tasks_csv_filepath)
        self.generator.write_csv(sequences, args.out_dependencies_csv_filepath)

        resolved.show(truncate=False)

def main():
    parser = argparse.ArgumentParser(description="Generate Task Sequences.")
    parser.add_argument("--task_proposals_json_filepath",
                        type=str,
                        required=True,
                        help="Path to the task proposals input JSON file")
    parser.add_argument("--out_tasks_csv_filepath",
                        type=str,
                        default="/data/generated/proposals/tasks.csv",
                        help="Path to the tasks output CSV file")
    parser.add_argument("--out_dependencies_csv_filepath",
                        type=str,
                        default="/data/generated/proposals/task_dependencies.csv",
                        help="Path to the dependencies output CSV file")

    parsed_args = parser.parse_args()
    app = JsonTaskApp()
    app.run(parsed_args)

if __name__ == "__main__":
    main()
