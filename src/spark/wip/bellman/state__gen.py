import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import argparse
from spark.base_gen import BaseGenerator
from spark.utils import format_task_args


class StateParamGen:
    def __init__(self, spark, param_config):
        self.spark = spark
        self.param_config = param_config

    def generate(self, dataframes):
        param_name = self.param_config['param_name']
        source = self.param_config['source']
        group_by_cols = self.param_config['group_by']
        value_col = self.param_config['value_col']
        aggregation = self.param_config.get('aggregation', 'max')  # Default to 'max'

        # Extract source DataFrame
        df = dataframes[source]

        # Apply the specified aggregation operation
        agg_expr = getattr(F, aggregation)(value_col).alias(param_name)
        aggregated = (
            df.groupBy(group_by_cols)
            .agg(agg_expr)
            .selectExpr(f"{group_by_cols[0]} as state_id", f"{param_name} as param_value")
            .withColumn("param_name", F.lit(param_name))
        )

        return aggregated


class StateGen(BaseGenerator):
    def __init__(self, spark, config):
        super().__init__(spark)
        self.config = config

    def run(self, dataframes):
        state_params = self.config['state_params']
        state_dataframes = []

        # Generate parameters based on configuration
        for param_config in state_params:
            param_gen = StateParamGen(self.spark, param_config)
            param_df = param_gen.generate(dataframes)
            state_dataframes.append(param_df)

        # Combine all parameters into a single state table
        state_table = self._combine_params(state_dataframes)
        return state_table

    def _combine_params(self, param_dfs):
        combined = None
        for df in param_dfs:
            if combined is None:
                combined = df
            else:
                combined = combined.union(df)
        return combined


class GenericStateGenApp:
    def __init__(self):
        self.spark = SparkSession.builder.appName("GenericStateGenApp").getOrCreate()

    def run(self, args):
        # Load configuration
        with open(args.state_yaml_filepath, 'r') as f:
            config = yaml.safe_load(f)

        generator = StateGen(self.spark, config)

        # Load data sources
        dataframes = {
            source['name']: self.spark.read.csv(source['path'], header=True, inferSchema=True)
            for source in config['data_sources']
        }

        # Generate state table
        state_table = generator.run(dataframes)

        # Write output
        state_table.write.csv(args.out_state_csv_filepath, header=True)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--branch",
                        type=str)
    parser.add_argument("--out_branch",
                        type=str)
    parser.add_argument("--state_yaml_filepath",
                        type=str,
                        default="/data/generated/{branch}/state.yaml")
    parser.add_argument("--out_state_csv_filepath",
                        type=str,
                        default="/data/generated/{out_branch}/state.csv")
    
    args = parser.parse_args()

    app = GenericStateGenApp()
    app.run(args)


if __name__ == "__main__":
    main()
