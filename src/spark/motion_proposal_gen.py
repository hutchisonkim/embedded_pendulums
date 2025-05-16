import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import shutil
from spark.base_gen import BaseGenerator

class MotionProposalGenerator(BaseGenerator):

    def generate_motion_proposals(self, motion_count: int):
        motion_proposals = (
            self.spark.range(0, motion_count)
            .withColumnRenamed("id", "motion_id")
            .withColumn("motion_id", F.expr("concat('motion_', motion_id)"))
            .withColumn("joint_count", F.lit(4))
        )

        return motion_proposals

class MotionProposalApp:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("MotionProposals") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        self.generator = MotionProposalGenerator(self.spark)

    def run(self, args):
        motions_data = self.generator.generate_motion_proposals(args.motion_count)

        self.generator.write_csv(motions_data, args.out_motion_proposals_csv_filepath)

        motions_data.show(truncate=False)

def main():
    parser = argparse.ArgumentParser(description="Generate motion proposals.")
    parser.add_argument("--motion_count",
                        type=int,
                        default=1,
                        help="Number of motions to generate")
    parser.add_argument("--out_motion_proposals_csv_filepath",
                        type=str,
                        default="/data/generated/proposals/motions.csv",
                        help="Path to the motions proposals output CSV file")

    parsed_args = parser.parse_args()
    app = MotionProposalApp()
    app.run(parsed_args)

if __name__ == "__main__":
    main()


