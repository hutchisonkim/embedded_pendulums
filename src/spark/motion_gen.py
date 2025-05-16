import argparse
from spark.base_gen import BaseGenerator
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

class MotionGenerator(BaseGenerator):

    def run(self, motion_proposals):
        motion_features, joint_features = self._generate(motion_proposals)
        motion_features, joint_features = self._populate(motion_features, joint_features)
        motions, joints = self._collapse(motion_features, joint_features)
        motions_json = self._group(motion_features, joint_features)
        return motions, joints, motions_json

    # Generate
    def _generate(self, motion_proposals):
        motion_features = self._generate_motion_features(motion_proposals)
        joint_features = self._generate_joint_features(motion_proposals)
        return motion_features, joint_features
    
    def _generate_motion_features(self, motion_proposals):
        return motion_proposals \
            .drop("joint_count") \

    def _generate_joint_features(self, motion_proposals):
        return motion_proposals \
            .withColumn("joint_index", F.explode(F.expr("sequence(0, joint_count - 1)"))) \
    
    # Configure
    def _populate(self, motion_features, joint_features):
        motion_features = self._populate_motion_features(motion_features)
        joint_features = self._populate_joint_features(joint_features)
        return motion_features, joint_features
    
    def _populate_motion_features(self, motion_features):
        return motion_features \
            .withColumn("root_orientation", F.lit(0.0)) \
            .withColumn("root_velocity_orientation", F.lit(np.pi * 1.1)) \
            .withColumn("root_velocity_magnitude", F.lit(1.0)) \
            
    def _populate_joint_features(self, joint_features):
        return joint_features \
            .withColumn("position_x", F.when(F.col("joint_index") == 0, 0.0).otherwise(F.lit(1.0))) \
            .withColumn("is_last_joint", F.when(F.col("joint_index") == F.col("joint_count") - 1, True).otherwise(False)) \
            .withColumn("orientation_amplitude", F.when(F.col("is_last_joint"), 0.0).otherwise(F.lit(0.2))) \
            .withColumn("orientation_phase", F.when(F.col("is_last_joint"), 0.0).otherwise(F.lit(np.pi * 2.0))) \
            .drop("is_last_joint") \
            .withColumn("orientation_phase_offset", F.lit(0.0)) \
    
    # Collapse
    def _collapse(self, motion_features, joint_features):
        motions = self._collapse_motion_features(motion_features)
        joints = self._collapse_joint_features(joint_features)
        return motions, joints
    
    def _collapse_motion_features(self, motion_features):
        return motion_features \

    def _collapse_joint_features(self, joint_features):
        return joint_features \
            .drop("joint_count") \

    # Group
    def _group(self, motion_features, joint_features):
        root_array = self._group_root_features(motion_features)
        joints_array = self._group_joint_features(joint_features)
        motions_json = self._group_motion_features(motion_features, root_array, joints_array)
        return motions_json
    
    def _group_root_features(self, root_features):
        return root_features \
            .withColumn("root", F.struct(
                F.col("root_orientation").alias("orientation"),
                F.col("root_velocity_orientation").alias("velocity_orientation"),
                F.col("root_velocity_magnitude").alias("velocity_magnitude"),
            )) \
            .drop("root_orientation", "root_velocity_orientation", "root_velocity_magnitude") \

    def _group_joint_features(self, joint_features):
        return joint_features \
            .groupBy("motion_id") \
            .agg(
                F.collect_list(
                    F.struct(
                        F.col("position_x"),
                        F.col("orientation_amplitude"),
                        F.col("orientation_phase"),
                        F.col("orientation_phase_offset"),
                    )
                ).alias("joints")
            ) \

    def _group_motion_features(self, motion_features, root_array, joints_array):
        return motion_features \
            .join(root_array, on="motion_id", how="left") \
            .join(joints_array, on="motion_id", how="left") \
            .withColumn(
                "motion",
                F.struct(
                    F.col("motion_id").alias("id"),
                    F.col("root"),
                    F.col("joints"),
                ),
            ) \
            .groupBy("motion_id") \
            .agg(F.collect_list("motion").alias("motions")) \
            .select("motions") \

class DefaultMotionGenerator(MotionGenerator):
    
    def _populate_motion_features(self, motion_features):
        return motion_features \
            .withColumn("root_orientation", F.lit(0.0)) \
            .withColumn("root_velocity_orientation", F.lit(np.pi * 1.1)) \
            .withColumn("root_velocity_magnitude", F.lit(1.0)) \
            
    def _populate_joint_features(self, joint_features):
        return joint_features \
            .withColumn("position_x", F.when(F.col("joint_index") == 0, 0.0).otherwise(F.lit(1.0))) \
            .withColumn("orientation_amplitude", F.when(F.col("joint_index") == F.col("joint_count") - 1, 0.0).otherwise(F.lit(0.2))) \
            .withColumn("orientation_phase", F.when(F.col("joint_index") == F.col("joint_count") - 1, 0.0).otherwise(F.lit(np.pi * 2.0))) \
            .withColumn("orientation_phase_offset", F.lit(0.0)) \


class MotionApp:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("MotionsAndJoints") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        self.generator = MotionGenerator(self.spark)

    def run(self, args):
        
        # Extract
        motion_proposals = self.generator.read_csv(args.in_motion_proposals_csv_filepath)

        # Transform
        motions, joints, motions_json = self.generator.run(motion_proposals)
        
        # Load
        self.generator.write_csv(motions, args.out_motions_csv_filepath)
        self.generator.write_csv(joints, args.out_joints_csv_filepath)
        self.generator.write_json(motions_json, args.out_motions_json_filepath)

        motions.show()
        joints.show()
        motions_json.show(truncate=False)
        motions_json.printSchema()


def main():
    parser = argparse.ArgumentParser(description="Generate motions and joints.")
    parser.add_argument("--in_motion_proposals_csv_filepath",
                        type=str,
                        default="/data/generated/proposals/motions.csv",
                        help="Path to the motions proposals input CSV file")
    parser.add_argument("--out_motions_json_filepath",
                        type=str,
                        default="/data/generated/motions.json",
                        help="Path to the motions output JSON file)")
    parser.add_argument("--out_motions_csv_filepath",
                        type=str,
                        default="/data/generated/motions.csv",
                        help="Path to the motions output CSV file")
    parser.add_argument("--out_joints_csv_filepath",
                        type=str,
                        default="/data/generated/motion_joints.csv",
                        help="Path to the joints output CSV file")

    args = parser.parse_args()
    app = MotionApp()
    app.run(args)

if __name__ == "__main__":
    main()


