import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, expr, format_string
from spark.base_gen import BaseGenerator

class JsonTaskGenerator2(BaseGenerator):
    def generate_permutations(self, data):
        """
        Generate all valid permutations based on dimensions and branches.
        """
        dimensions = data["dimensions"]
        branches = data["branches"]

        # Create base permutations
        rows = []
        for asset in dimensions["asset"]:
            for stage in branches[asset]:
                for operator in branches[stage]:
                    rows.append((asset, stage, operator, None))
                    if operator in branches:
                        for post in branches[operator]:
                            rows.append((asset, stage, operator, post))

        # Convert to Spark DataFrame
        return self.spark.createDataFrame(rows, schema=["asset", "stage", "operator", "post"])

    def resolve_paths(self, df, paths):
        """
        Resolve paths dynamically based on the given conditions and templates.
        """
        for path_key, path_template in paths.items():
            conditions = path_key.split("&")
            column_name = f"path_{path_key.replace('&', '_')}"

            # Build condition expression
            condition_expr = " AND ".join(
                [f"({key} = '{value}')" for cond in conditions if "=" in cond for key, value in [cond.split("=")]]
            )

            # Dynamically format paths
            resolved_path = path_template
            for placeholder in ["asset", "stage", "operator", "post"]:
                resolved_path = resolved_path.replace(f"{{{placeholder}}}", f"%s")

            # Add the resolved path as a new column
            df = df.withColumn(
                column_name,
                when(expr(condition_expr), format_string(resolved_path, col("asset"), col("stage"), col("operator"), col("post"))).otherwise(None)
            )
        return df
    
    def resolve_task_id(self, df):
        # Create a 'task' column by concatenating 'asset', 'stage', 'operator', and 'post' with underscores
        #df['task'] = df[['asset', 'stage', 'operator', 'post']].fillna('').agg('_'.join, axis=1).str.strip('_')
        df = df.withColumn(
            "task",
            when(col("post").isNotNull(), format_string("%s_%s_%s_%s", col("asset"), col("stage"), col("operator"), col("post")))
            .otherwise(format_string("%s_%s_%s", col("asset"), col("stage"), col("operator")))
        )
        return df

    def run(self, data):
        """
        Generate permutations and resolve paths.
        """
        # Step 1: Generate all valid permutations
        df = self.generate_permutations(data)

        # Step 2: Resolve paths based on columns
        df = self.resolve_paths(df, data["paths"])

        # Step 3: Resolve task IDs
        df = self.resolve_task_id(df)

        return df


class JsonTaskApp2:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("DagGeneration") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        self.generator = JsonTaskGenerator2(self.spark)

    def run(self, args):
        # Sample JSON data
        data = {
            "dimensions": {
                "asset": ["motion", "animation"],
                "stage": ["proposal", "manifest", "preview"],
                "operator": ["generator", "validator"],
                "post": ["test", "sensor"]
            },
            "branches": {
                "motion": ["proposal", "manifest"],
                "animation": ["proposal", "manifest", "preview"],
                "proposal": ["generator", "validator"],
                "manifest": ["generator", "validator"],
                "preview": ["generator", "validator"],
                "generator": ["test", "sensor"]
            },
            "paths": {
                "asset&stage=proposal": "/data/generated/proposals/{asset}s.csv",
                "asset&stage=manifest": "/data/generated/{asset}s.csv",
                "asset&stage=preview": "/data/generated/previews/{asset}s.png",
                "asset&stage&operator=generator": "/src/spark/{asset}_{stage}_gen.py",
                "asset&stage&operator=generator&post=test": "/src/spark/test/{asset}_{stage}_gen_test.py",
                "asset&stage&operator=validator": "/src/spark/validator/{asset}_{stage}_validator.py"
            },
            "dependencies":  [
                [
                    { "name": "Stage Sequence" },
                    { "partition": "asset" },
                    { "filter": { "operator": "generator" } },
                    { "filter": { "post": "" } },
                    { "order": { "stage": ["proposal", "manifest", "preview"] } },
                ],
                [
                    { "name": "Asset Sequence" },
                    { "filter": { "stage": ["proposal", "manifest"] } },
                    { "filter": { "operator": "generator" } },
                    { "order": { "asset": ["motion", "animation"] } },
                ],
                [
                    { "name": "Generator to Validator" },
                    { "partition": ["asset", "stage", "operator"] },
                    { "filter": { "operator": ["generator", "validator"] } },
                    { "filter": { "post": "" } },
                    { "order": { "operator": ["generator", "validator"] } },
                ],
                [
                    { "name": "Test to Operator" },
                    { "partition": ["asset", "stage", "operator"] },
                    { "filter": { "operator": "generator" } },
                    { "filter": { "post": ["test", ""] } },
                    { "order": { "post": ["test", ""] } },
                ],
                [
                    { "name": "Sensor to Test" },
                    { "partition": ["asset", "stage", "operator"] },
                    { "filter": { "operator": "generator" } },
                    { "filter": { "post": ["sensor", "test"] } },
                    { "order": { "post": ["sensor", "test"] } },
                ],
            ],
        }

        # Generate permutations and resolve paths
        df_with_paths = self.generator.run(data)

        # Show the resulting DataFrame
        df_with_paths.show(20, truncate=False)

        # Write the resulting DataFrame to a CSV file
        output_csv_path = args.out_dags_csv_filepath
        self.generator.write_csv(df_with_paths, output_csv_path)


def main():
    parser = argparse.ArgumentParser(description="Generate DAG paths.")
    parser.add_argument("--out_dags_csv_filepath",
                        type=str,
                        default="/data/generated/dags.csv",
                        help="Path to the dags output CSV file")
    parser.add_argument("--out_tasks_csv_filepath",
                        type=str,
                        default="/data/generated/tasks.csv",
                        help="Path to the tasks output CSV file")
    parser.add_argument("--out_dependencies_csv_filepath",
                        type=str,
                        default="/data/generated/dependencies.csv",
                        help="Path to the dependencies output CSV file")
    args = parser.parse_args()
    app = JsonTaskApp2()
    app.run(args)

if __name__ == "__main__":
    main()





