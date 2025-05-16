import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from spark.base_gen import BaseGenerator

class AnimationProposalGenerator(BaseGenerator):

    def sweep(self, joints: DataFrame, resolution: int, last_effector_only: bool = False) -> DataFrame:
        lod_index = 0
        frame_count = resolution * (2 ** lod_index)
        end_frame_index = frame_count - 1
        total_time = 1.0
        h = total_time / frame_count
        end_frame_time = total_time - h
        #TODO: fence against division by zero
        time_factor = end_frame_time / end_frame_index


        animation_proposals = joints \
            .select("motion_id") \
            .dropDuplicates() \
            .withColumn("animation_id", F.concat(F.lit("anim_"), F.row_number().over(Window.partitionBy("motion_id").orderBy("motion_id")))) \
            .withColumn("resolution", F.lit(resolution)) \
            .select("animation_id", "motion_id", "resolution") \

        print("animation_proposals:")
        animation_proposals.printSchema()
        animation_proposals.show(truncate=False)
            

        frame_proposals = animation_proposals \
            .withColumn("lod_index", F.lit(lod_index)) \
            .withColumn("frame_index", F.explode(F.sequence(F.lit(0), F.lit(end_frame_index), F.lit(1)))) \
            .withColumn("time", F.expr(f"frame_index * {time_factor}")) \
            .withColumn("frame_id", F.expr("concat('frame_', monotonically_increasing_id() + 1)")) \
            .select("animation_id", "lod_index", "frame_index", "time", "frame_id") \

        print("frame_proposals:")
        frame_proposals.printSchema()
        frame_proposals.show(truncate=False)
        
        #TODO: refactor joints as a tree with multiple children per parent, each in wave form relative to time
        return animation_proposals, frame_proposals
    
    def refine(self, animations: DataFrame):

        pivots = animations \
            .filter((F.col("is_positive_peak") | F.col("is_negative_peak") | F.col("is_crossover"))) \
            .withColumn("lod_index", F.expr("lod_index + 1")) \
            .withColumn("frame_count", F.expr("resolution * pow(2, lod_index)")) \
            .withColumn("end_frame_index", F.expr("frame_count - 1")) \
            .withColumn("total_time", F.expr("end_frame_time - time")) \
            .withColumn("h", F.expr("total_time / frame_count")) \
            .withColumn("end_frame_time", F.expr("total_time - h")) \
            .withColumn("time_factor", F.expr("end_frame_time / end_frame_index")) \
            .withColumn("frame_index", F.expr("frame_index * 2")) \
            .withColumn("time", F.expr("frame_index * time_factor")) \
            .drop("frame_count", "end_frame_index", "total_time", "end_frame_time") \
            
        # compact_pivots = pivots \
        #     .groupBy(
        #         "animation_id",
        #         "motion_id",
        #         "lod_index",
        #         "frame_index",
        #         "resolution",
        #     ).agg(
        #         F.max("effector_index").alias("max_effector_index")
        #     ).drop("time") \
        #     .drop("x", "y", "curvature") \
        #     .drop("is_positive_peak", "is_negative_peak", "is_crossover") \

        subdivisions = pivots \
            .withColumn("signs", F.array(F.lit(-1), F.lit(1))) \
            .withColumn("sign", F.explode(F.col("signs"))) \
            .withColumn("time", F.expr("time + (sign * h)")) \
            .withColumn("frame_index", F.expr("frame_index + (sign * 1)").cast("int")) \
            .drop("signs", "sign", "h") \
            .withColumn("x", F.lit(None)) \
            .withColumn("y", F.lit(None)) \
            .withColumn("curvature", F.lit(None)) \
            .withColumn("is_positive_peak", F.lit(None)) \
            .withColumn("is_negative_peak", F.lit(None)) \
            
        # subdivisions = compact_subdivisions \
        #     .withColumn("effector_index", F.expr("sequence(0, max_effector_index)")) \
        #     .withColumn("effector_index", F.explode(F.col("effector_index"))) \
        #     .drop("max_effector_index") \
        #     .withColumn("x", F.lit(None)) \
        #     .withColumn("y", F.lit(None)) \
        #     .withColumn("curvature", F.lit(None)) \
        #     .withColumn("is_positive_peak", F.lit(None)) \
        #     .withColumn("is_negative_peak", F.lit(None)) \

        animation_proposals = subdivisions \
            .unionByName(pivots, allowMissingColumns=True) \
            .orderBy("animation_id", "lod_index", "frame_index") \

        return animation_proposals


class AnimationProposalApp:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("AnimationProposals") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        self.generator = AnimationProposalGenerator(self.spark)

    def run(self, args):

        if args.is_refine_mode and args.is_sweep_mode:
            raise ValueError("Specify either sweep mode or refine mode, not both.")
        if not args.is_refine_mode and not args.is_sweep_mode:
            raise ValueError("Specify either sweep mode or refine mode.")
        
        if args.is_sweep_mode:
            joints = self.generator.read_csv(args.in_joints_csv_filepath)
            animation_proposals, frame_proposals = self.generator.sweep(joints, args.resolution)

        if args.is_refine_mode:
            animations = self.generator.read_csv(args.in_animations_csv_filepath)
            animation_proposals, frame_proposals = self.generator.refine(animations)

        self.generator.write_csv(animation_proposals, args.out_animation_proposals_csv_filepath)
        self.generator.write_csv(frame_proposals, args.out_frame_proposals_csv_filepath)

        animation_proposals.show(truncate=False)


def main():
    parser = argparse.ArgumentParser(description="Generate animation proposals.")
    
    # Any mode
    parser.add_argument("--out_animation_proposals_csv_filepath",
                        type=str,
                        default="/data/generated/proposals/animations.csv",
                        help="Path to the animation proposals output CSV file")
    parser.add_argument("--out_frame_proposals_csv_filepath",
                        type=str,
                        default="/data/generated/proposals/animation_frames.csv",
                        help="Path to the frame proposals output CSV file")

    # Sweep mode
    parser.add_argument("--is_sweep_mode",
                        action='store_true',
                        default=True,
                        help="Specify whether to use sweep mode")
    parser.add_argument("--in_joints_csv_filepath",
                        type=str,
                        default="/data/generated/motion_joints.csv", 
                        help="Path to the joints input CSV file (sweep mode only)")
    parser.add_argument("--resolution", type=int,
                        default=20,
                        help="Resolution for the animation proposals (sweep mode only)")

    # Subdivide mode
    parser.add_argument("--is_refine_mode",
                        action='store_true',
                        default=False,
                        help="Specify whether to use refine mode")
    parser.add_argument("--in_animations_csv_filepath",
                        type=str,
                        default="/data/generated/animations_lod0.csv",
                        help="Path to the animations input CSV file (refine mode only)")

    parsed_args = parser.parse_args()
    app = AnimationProposalApp()
    app.run(parsed_args)

if __name__ == "__main__":
    main()