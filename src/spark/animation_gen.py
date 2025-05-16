import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from spark.base_gen import BaseGenerator

class AnimationGenerator(BaseGenerator):

    #TODO: refactor roots as a zero-amplitude sine wave with a offset, where the phase offset is the root orientation. this will allow for a more general solution to the problem of unparenting.
    def _translate(self, animation_roots: DataFrame) -> DataFrame:

        animation_roots = animation_roots \
            .withColumn("root_position_x", F.col("root_velocity_magnitude") * F.cos(F.col("root_velocity_orientation")) * F.col("time")) \
            .withColumn("root_position_y", F.col("root_velocity_magnitude") * F.sin(F.col("root_velocity_orientation")) * F.col("time")) \
            .drop("motion_id", "root_velocity_magnitude", "root_velocity_orientation") \
            
        print("_translate()")
        print("animation_roots:")
        animation_roots.show()
        return animation_roots
            

    def _oscillate(self, animation_joints: DataFrame) -> DataFrame:

        animation_joints = animation_joints \
            .withColumn("orientation", F.col("orientation_amplitude") * F.sin(F.col("orientation_phase") * F.col("time") + F.col("orientation_phase_offset"))) \
            .withColumn("position_y", F.lit(0.0)) \
            .drop("orientation_amplitude", "orientation_phase", "orientation_phase_offset") \
            
        print("_oscillate()")
        print("animation_joints:")
        animation_joints.show()
        return animation_joints
    
    def _unparent_linear_window_lag(self, animation_joints: DataFrame) -> DataFrame:
        """
        Unparents animation joints by calculating global transforms iteratively
        using window functions and lag based on exploded columns like joint_index.
        """
        # Define the initial global effector state
        initial_effector_state = F.struct(
            F.lit(0.0).alias("effector_position_x"),
            F.lit(0.0).alias("effector_position_y"),
            F.lit(0.0).alias("effector_orientation")
        )

        # Define a window to process joints in order of joint_index for each motion_id
        window_spec = (
            F.window()
            .partitionBy("motion_id")
            .orderBy("joint_index")
        )

        # Calculate global transforms iteratively
        animation_joints = animation_joints.withColumn(
            "global_transforms",
            F.when(
                F.col("joint_index") == 0,  # For the root joint (index 0), use the initial state
                initial_effector_state
            ).otherwise(
                F.struct(
                    (F.lag("global_transforms.effector_position_x").over(window_spec) +
                    F.col("local_position_x") * F.cos(F.lag("global_transforms.effector_orientation").over(window_spec))).alias("effector_position_x"),
                    (F.lag("global_transforms.effector_position_y").over(window_spec) +
                    F.col("local_position_y") * F.sin(F.lag("global_transforms.effector_orientation").over(window_spec))).alias("effector_position_y"),
                    (F.lag("global_transforms.effector_orientation").over(window_spec) +
                    F.col("local_orientation")).alias("effector_orientation")
                )
            )
        ) \
        



        print("_unparent_linear_window_lag()")
        print("animation_joints:")
        animation_joints.show()
        return animation_joints

    def _unparent_linear_aggregate(self, animation_joints: DataFrame) -> DataFrame:
        """
        Unparents animation joints by calculating global transforms iteratively
        using Spark functions instead of a large expression string.
        Adds a step to group rows and create the joints struct.
        """

        print("------------------------------")
        print("_unparent_linear_aggregate()")

        animation_joints = animation_joints \
            .withColumnRenamed("position_x", "local_position_x") \
            .withColumnRenamed("position_y", "local_position_y") \
            .withColumnRenamed("orientation", "local_orientation") \
            
        print("animation_joints:")
        animation_joints.show()

        # Step 1: Group rows to create the joints struct
        animation_joints_array = animation_joints \
            .orderBy("animation_id", "frame_id", "joint_index") \
            .groupBy("animation_id", "frame_id") \
            .agg(
                F.collect_list(
                    F.struct(
                        F.col("local_position_x"),
                        F.col("local_position_y"),
                        F.col("local_orientation"),
                    )
                ).alias("joints")
            ) \
            .withColumn(
                "joints",
                F.expr("transform(joints, x -> struct(x.local_position_x, x.local_position_y, x.local_orientation))")
            ) \
            
        print("animation_joints_array:")
        animation_joints_array.show()
            
        animation_joints_array = animation_joints_array \
            .join(
                animation_joints.select("animation_id", "motion_id").distinct(),
                on="animation_id",
                how="left"
            )
        
        print("animation_joints_array:")
        animation_joints_array.show()

        # Step 2: Define the initial global effector state
        initial_effector_state = F.array(
            F.struct(
                F.lit(0.0).alias("effector_position_x"),
                F.lit(0.0).alias("effector_position_y"),
                F.lit(0.0).alias("effector_orientation")
            )
        )

        # Step 3: Define the aggregation function
        def update_global_effectors(global_effectors, joint):
            last_effector = F.element_at(global_effectors, -1)
            new_effector = F.struct(
                (last_effector.effector_position_x + joint.local_position_x * F.cos(last_effector.effector_orientation)).alias("effector_position_x"),
                (last_effector.effector_position_y + joint.local_position_x * F.sin(last_effector.effector_orientation)).alias("effector_position_y"),
                (last_effector.effector_orientation + joint.local_orientation).alias("effector_orientation")
            )
            return F.concat(global_effectors, F.array(new_effector))

        # Step 4: Apply the aggregation
        animation_joints_array = animation_joints_array \
            .withColumn(
                "global_transforms",
                F.aggregate(
                    F.col("joints"),  # Input array column
                    initial_effector_state,  # Initial value
                    update_global_effectors  # Lambda function for aggregation
                )
            ) \
            .withColumn(
                "global_transforms",
                F.slice(F.col("global_transforms"), 2, F.size(F.col("global_transforms")) - 1)
            ) \
            

        print("animation_joints_array:")
        animation_joints_array.show()

        # Step 5: Explode using explode pos the global transforms to get individual rows for each joint, along with the corresponding effector_index
        animation_effectors = animation_joints_array \
            .select(
                "animation_id",
                "motion_id",
                "frame_id",
                F.posexplode("global_transforms").alias("effector_index", "global_transform")
            ) \
            .withColumn("effector_position_x", F.col("global_transform.effector_position_x")) \
            .withColumn("effector_position_y", F.col("global_transform.effector_position_y")) \
            .withColumn("effector_orientation", F.col("global_transform.effector_orientation")) \
            .drop("global_transform") \

        print("animation_effectors:")
        animation_effectors.show()

        print("_unparent_linear_aggregate()")
        print("------------------------------")
        
        return animation_effectors

    def _unparent_linear_old(self, animation_joints: DataFrame) -> DataFrame:
        #TODO: decompose into smaller functions
        aggregation_expr = """
            aggregate(
                joints,
                array(struct(
                    root.x AS effector_position_x,
                    root.y AS effector_position_y,
                    cast(0.0 AS DOUBLE) AS effector_orientation
                )),
                (global_effectors, joint) -> concat(
                    global_effectors,
                    array(
                        struct(
                            (element_at(global_effectors, -1).effector_position_x + joint.local_position_x * cos(element_at(global_effectors, -1).effector_orientation)) AS effector_position_x,
                            (element_at(global_effectors, -1).effector_position_y + joint.local_position_x * sin(element_at(global_effectors, -1).effector_orientation)) AS effector_orientation
                        )
                    )
                )
            )
        """

        return animation_joints.withColumn("global_transforms", F.expr(aggregation_expr))
    

    def _unparent_hierarchical(self, joints: DataFrame) -> DataFrame:

        effectors = joints.filter(F.col("parent_id").isNull()) \
            .withColumnRenamed("local_position_x", "position_x") \
            .withColumnRenamed("local_position_y", "position_y") \
            .withColumnRenamed("local_orientation", "orientation")

        remaining_joints = joints.filter(F.col("parent_id").isNotNull())

        iteration_count = 0
        while not remaining_joints.isEmpty() and iteration_count < 10:
            iteration_count += 1
            computed = remaining_joints \
                .join(effectors, remaining_joints.parent_id == effectors.joint_id) \
                .select(
                    F.col("remaining_joints.joint_id"),
                    F.col("remaining_joints.parent_id"),
                    (F.col("position_x") + F.col("local_position_x") * F.cos(F.col("orientation"))).alias("position_x"),
                    (F.col("position_y") + F.col("local_position_y") * F.sin(F.col("orientation"))).alias("position_y"),
                    (F.col("orientation") + F.col("local_orientation")).alias("orientation")
                )
            
            effectors = effectors.union(computed)
            
            remaining_joints = remaining_joints.join(computed, remaining_joints.joint_id == computed.joint_id, "left_anti")

            print(f"iteration_count={iteration_count}, remaining_joints.count()={remaining_joints.count()}")
            print("remaining_joints=")
            remaining_joints.show()

        print("effectors=")
        effectors.show()

        return effectors

    def run(self,
                motions: DataFrame,
                motion_joints: DataFrame,
                animation_proposals: DataFrame,
                animation_frame_proposals: DataFrame,
                ) -> DataFrame:
        
        animations = animation_proposals
        animation_frames = animation_frame_proposals

        # roots
        animation_root_proposals = motions \
            .join(animation_proposals, on="motion_id", how="left") \
            .join(animation_frame_proposals, on="animation_id", how="left") \
            .select("motion_id", "root_orientation", "root_velocity_orientation", "root_velocity_magnitude", "animation_id", "frame_id", "time") \
            .distinct() \

        print("animation_root_proposals:")
        animation_root_proposals.show()

        animation_roots = self._translate(animation_root_proposals)

        print("animation_roots:")
        animation_roots.show()

        
        # joints
        animation_joint_proposals = motions \
            .join(motion_joints, on="motion_id", how="left") \
            .join(animation_proposals.select("animation_id", "motion_id"), on="motion_id", how="left") \
            .join(animation_frame_proposals.select("animation_id", "frame_id", "time"), on="animation_id", how="left") \
            .select("motion_id", "joint_index", "position_x", "orientation_amplitude", "orientation_phase", "orientation_phase_offset", "animation_id", "frame_id", "time") \
            
        print("animation_joint_proposals:")
        animation_joint_proposals.show()

        animation_joints = self._oscillate(animation_joint_proposals) \

        print("animation_joints:")
        animation_joints.show()

        # effectors
        animation_effectors = self._unparent_linear_aggregate(animation_joints) \
        
        print("animation_effectors:")
        animation_effectors.show()

        animation_effectors = animation_effectors \
            .join(animation_roots, on=["animation_id", "frame_id"], how="left") \
            .withColumn("effector_position_x", F.col("effector_position_x") + F.col("root_position_x")) \
            .withColumn("effector_position_y", F.col("effector_position_y") + F.col("root_position_y")) \
            

        print("animation_effectors:")
        animation_effectors.show()

        # animations array
        animation_effectors_array = animation_effectors \
            .groupBy("animation_id", "frame_id") \
            .agg(
                F.collect_list(
                    F.struct(
                        F.col("effector_position_x").alias("position_x"),
                        F.col("effector_position_y").alias("position_y"),
                        F.col("effector_orientation").alias("orientation"),
                    )
                ).alias("effectors")
            )
        
        animation_joints_array = animation_joints \
            .groupBy("animation_id", "frame_id") \
            .agg(
                F.collect_list(
                    F.struct(
                        F.col("position_x").alias("position_x"),
                        F.col("orientation").alias("orientation"),
                    )
                ).alias("joints"),
            )
                
        animations_array = animation_effectors_array \
            .join(animation_joints_array, on=["animation_id", "frame_id"], how="left") \

        # join with animations to get the motion id
        animations_array = animations_array \
            .join(animations.select("animation_id", "motion_id"), on="animation_id", how="left") \
            
        # join with animation frame proposals to get the time from the animation frames, on ["animation_id", "frame_id"]
        animations_array = animations_array \
            .join(animation_frames.select("animation_id", "frame_id", "time"), on=["animation_id", "frame_id"], how="left") \

        # create a frames array from the frame_id column. each element of the array has a joints and effectors array
        animations_array = animations_array \
            .orderBy("animation_id", "time") \
            .groupBy("animation_id") \
            .agg(
                F.collect_list(
                    F.struct(
                        F.col("time"),
                        F.col("effectors"),
                        F.col("joints")
                    )
                ).alias("frames")
            ) \
        
        # put the array inside a "animations" struct
        animations_json = animations_array \
            .withColumn("animation", F.struct(
                F.col("animation_id").alias("id"),
                F.col("frames")
            )) \
            .groupBy("animation_id") \
            .agg(F.collect_list("animation").alias("animations")) \
            .select("animations") \

        # clean up the frame columns
        animation_effectors = animation_effectors \
            .drop("motion_id", "frame_index", "root_orientation", "root_position_x", "root_position_y", "time") \

        return animations, animation_frames, animation_joint_proposals, animation_effectors, animations_json


class AnimationApp:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("Animations") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        self.generator = AnimationGenerator(self.spark)

    def run(self, args):

        motions = self.generator.read_csv(args.in_motions_csv_filepath)
        motion_joints = self.generator.read_csv(args.in_motion_joints_csv_filepath)
        animation_proposals = self.generator.read_csv(args.in_animation_proposals_csv_filepath)
        animation_frame_proposals = self.generator.read_csv(args.in_animation_frame_proposals_csv_filepath)

        animations, animation_frames, animation_joints, animation_effectors, animations_json = self.generator.run(motions, motion_joints, animation_proposals, animation_frame_proposals)

        self.generator.write_csv(animations, args.out_animations_csv_filepath)
        self.generator.write_csv(animation_frames, args.out_animation_frames_csv_filepath)
        self.generator.write_csv(animation_joints, args.out_animation_joints_csv_filepath)
        self.generator.write_csv(animation_effectors, args.out_animation_effectors_csv_filepath)
        self.generator.write_json(animations_json, args.out_animations_json_filepath)

        animations.show()
        animation_frames.show()
        animation_joints.show()
        animation_effectors.show()


def main():
    parser = argparse.ArgumentParser(description="Generate animations and frames.")

    parser.add_argument("--in_motions_csv_filepath",
                        type=str,
                        default="/data/generated/motions.csv",
                        help="Path to the motions input CSV file")
    parser.add_argument("--in_motion_joints_csv_filepath",
                        type=str,
                        default="/data/generated/motion_joints.csv",
                        help="Path to the motion joints input CSV file")
    
    parser.add_argument("--in_animation_proposals_csv_filepath",
                        type=str,
                        default="/data/generated/proposals/animations.csv",
                        help="Path to the animation proposals input CSV file")
    parser.add_argument("--in_animation_frame_proposals_csv_filepath",
                        type=str,
                        default="/data/generated/proposals/animation_frames.csv",
                        help="Path to the animation frame proposals input CSV file")
    
    parser.add_argument("--out_animations_csv_filepath",
                        type=str,
                        default="/data/generated/animations.csv",
                        help="Path to the animations output CSV file")
    parser.add_argument("--out_animation_frames_csv_filepath",
                        type=str,
                        default="/data/generated/animation_frames.csv",
                        help="Path to the frames output CSV file")
    parser.add_argument("--out_animation_joints_csv_filepath",
                        type=str,
                        default="/data/generated/animation_joints.csv",
                        help="Path to the joints output CSV file")
    parser.add_argument("--out_animation_effectors_csv_filepath",
                        type=str,
                        default="/data/generated/animation_effectors.csv",
                        help="Path to the effectors output CSV file")
    
    parser.add_argument("--out_animations_json_filepath",
                        type=str,
                        default="/data/generated/animations.json",
                        help="Path to the animations output JSON file")

    args = parser.parse_args()
    app = AnimationApp()
    app.run(args)

if __name__ == "__main__":
    main()


