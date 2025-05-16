import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from spark.base_gen import BaseGenerator
import numpy as np

class SplineGenerator(BaseGenerator):
    def compute_curvatures(self, effectors, frames):

        # Define a window specification to partition by animation_id and effector_index
        window_spec = Window.partitionBy("animation_id", "effector_index").orderBy("time")

        # remove effectors with index 0 (linear motion creates no curvature)
        effectors = effectors.filter(F.col("effector_index") != 0)

        # join with frames to get time
        effectors = effectors \
            .drop("time") \
            .join(frames, ["animation_id", "frame_id"], "inner") \
                .select("animation_id", "effector_index", "frame_id", "time",
                        "effector_position_x", "effector_position_y", "effector_orientation")


        # Compute previous and next positions
        effectors = effectors \
            .withColumn("prev_x", F.lag("effector_position_x", 1).over(window_spec)) \
            .withColumn("prev_y", F.lag("effector_position_y", 1).over(window_spec)) \
            .withColumn("next_x", F.lead("effector_position_x", 1).over(window_spec)) \
            .withColumn("next_y", F.lead("effector_position_y", 1).over(window_spec))

        # Compute curvature using the formula
        curvatures = effectors \
            .withColumn(
                "curvature",
                F.expr("""
                    CASE
                        WHEN prev_x IS NULL OR prev_y IS NULL THEN 0.0
                        WHEN next_x IS NULL OR next_y IS NULL THEN 0.0
                        WHEN pow((next_x - effector_position_x), 2) + pow((next_y - effector_position_y), 2) = 0 THEN 0.0
                        ELSE ((effector_position_x - prev_x) * (next_y - effector_position_y) - (effector_position_y - prev_y) * (next_x - effector_position_x)) /
                            pow(pow((effector_position_x - prev_x), 2) + pow((effector_position_y - prev_y), 2), 1.5)
                    END
                """)
            )

        curvatures_with_time = curvatures \
            .select("animation_id", "effector_index", "frame_id", "time", "curvature") \
            .orderBy("animation_id", "effector_index", "time") \

        return curvatures_with_time

    def compute_peaks(self, curvatures):
        """
        Identify peaks and crossovers in the curvature data.

        Parameters:
            curvatures (DataFrame): DataFrame containing curvature data.

        Returns:
            DataFrame: DataFrame with identified peaks and crossovers.
        """
        # Identify positive and negative peaks
        peaks = (
            curvatures
            .withColumn(
                "max_curvature",
                F.max("curvature").over(Window.partitionBy("animation_id", "effector_index"))
            )
            .withColumn(
                "min_curvature",
                F.min("curvature").over(Window.partitionBy("animation_id", "effector_index"))
            )
            .withColumn(
                "is_positive_peak",
                F.expr("curvature = max_curvature")
            )
            .withColumn(
                "is_negative_peak",
                F.expr("curvature = min_curvature")
            )
            .withColumn(
                "curvature_sign_change",
                F.expr("curvature * lag(curvature, 1) over (partition by animation_id, effector_index order by time)")
            )
            .withColumn(
                "is_crossover",
                F.expr("curvature_sign_change < 0")
            )
        )

        # remove rows that are neither positive nor negative peaks nor crossovers
        peaks = peaks.filter(
            (F.col("is_positive_peak") | F.col("is_negative_peak") | F.col("is_crossover"))
        )

        # Select relevant columns
        peaks = peaks \
            .orderBy("animation_id", "effector_index", "time") \
            .withColumn("peak_type",
                F.when(F.col("is_positive_peak"), "positive")
                .when(F.col("is_negative_peak"), "negative")
                .when(F.col("is_crossover"), "crossover")
            ) \
            .select(
                "animation_id",
                "effector_index",
                "frame_id",
                "time",
                "peak_type",
            ) \


        return peaks

    def compute_tangents(self, effectors, frames):

        # Define a window specification to partition by animation_id and effector_index
        window_spec = Window.partitionBy("animation_id", "effector_index").orderBy("time")

        # remove effectors with index 0 (linear motion creates no curvature)
        effectors = effectors.filter(F.col("effector_index") != 0)

        # join with frames to get time
        effectors = effectors \
            .drop("time") \
            .join(frames, ["animation_id", "frame_id"], "inner") \
                .select("animation_id", "effector_index", "frame_id", "time",
                        "effector_position_x", "effector_position_y", "effector_orientation")
        
        # Compute previous and next positions
        effectors = effectors \
            .withColumn("prev_x", F.lag("effector_position_x", 1).over(window_spec)) \
            .withColumn("prev_y", F.lag("effector_position_y", 1).over(window_spec)) \
            .withColumn("next_x", F.lead("effector_position_x", 1).over(window_spec)) \
            .withColumn("next_y", F.lead("effector_position_y", 1).over(window_spec))
        
        # Compute tangents using the formula
        tangents = effectors \
            .withColumn(
                "tangent",
                F.expr("""
                    CASE
                        WHEN prev_x IS NULL OR prev_y IS NULL THEN 0.0
                        WHEN next_x IS NULL OR next_y IS NULL THEN 0.0
                        WHEN pow((next_x - effector_position_x), 2) + pow((next_y - effector_position_y), 2) = 0 THEN 0.0
                        ELSE atan2(next_y - effector_position_y, next_x - effector_position_x)
                    END
                """)
            )
        
        return tangents \
            .select("animation_id", "effector_index", "frame_id", "time", "tangent") \
            .orderBy("animation_id", "effector_index", "time") \
            

    def compute_bisectors(self, effectors, frames):
        # Define a window specification to partition by animation_id and effector_index
        window_spec = Window.partitionBy("animation_id", "effector_index").orderBy("time")

        # remove effectors with index 0 (linear motion creates no curvature)
        effectors = effectors.filter(F.col("effector_index") != 0)
        # effectors = effectors.filter(F.col("effector_index") == 3)

        # join with frames to get time
        effectors = effectors \
            .drop("time") \
            .join(frames, ["animation_id", "frame_id"], "left") \
            .select("animation_id", "effector_index", "frame_id", "time",
                    "effector_position_x", "effector_position_y", "effector_orientation") \
            .orderBy("animation_id", "effector_index", "time") \
        
        # Compute previous and next positions
        effectors = effectors \
            .withColumn("prev_x", F.lag("effector_position_x", 1).over(window_spec)) \
            .withColumn("prev_y", F.lag("effector_position_y", 1).over(window_spec)) \
            .withColumn("next_x", F.lead("effector_position_x", 1).over(window_spec)) \
            .withColumn("next_y", F.lead("effector_position_y", 1).over(window_spec)) \
        

        # get the point half way between the previous and next points
        bisectors = effectors \
            .withColumn("delta_prev_x", F.col("prev_x") - F.col("effector_position_x")) \
            .withColumn("delta_prev_y", F.col("prev_y") - F.col("effector_position_y")) \
            .withColumn("delta_prev_magnitude", F.sqrt(F.pow(F.col("delta_prev_x"), 2) + F.pow(F.col("delta_prev_y"), 2))) \
            .withColumn("unit_delta_prev_x", F.col("delta_prev_x") / F.col("delta_prev_magnitude")) \
            .withColumn("unit_delta_prev_y", F.col("delta_prev_y") / F.col("delta_prev_magnitude")) \
            .withColumn("delta_next_x", F.col("next_x") - F.col("effector_position_x")) \
            .withColumn("delta_next_y", F.col("next_y") - F.col("effector_position_y")) \
            .withColumn("delta_next_magnitude", F.sqrt(F.pow(F.col("delta_next_x"), 2) + F.pow(F.col("delta_next_y"), 2))) \
            .withColumn("unit_delta_next_x", F.col("delta_next_x") / F.col("delta_next_magnitude")) \
            .withColumn("unit_delta_next_y", F.col("delta_next_y") / F.col("delta_next_magnitude")) \
            .withColumn("mid_x", (F.col("unit_delta_prev_x") + F.col("unit_delta_next_x")) / 2) \
            .withColumn("mid_y", (F.col("unit_delta_prev_y") + F.col("unit_delta_next_y")) / 2) \
            .withColumn("mid_magnitude", F.sqrt(F.pow(F.col("mid_x"), 2) + F.pow(F.col("mid_y"), 2))) \
            .withColumn("unit_mid_x", F.col("mid_x") / F.col("mid_magnitude")) \
            .withColumn("unit_mid_y", F.col("mid_y") / F.col("mid_magnitude")) \
            .withColumn("bisector_x", F.col("unit_mid_x")) \
            .withColumn("bisector_y", F.col("unit_mid_y")) \
            
        #add a bisector magnitude column that is linear relative to the angular distance between the two previous and next delta vectors
        bisectors = bisectors \
            .withColumn("delta_prev_angle", F.atan2(F.col("unit_delta_prev_y"), F.col("unit_delta_prev_x"))) \
            .withColumn("delta_next_angle", F.atan2(F.col("unit_delta_next_y"), F.col("unit_delta_next_x"))) \
            .withColumn("angle_diff", F.abs(F.col("delta_prev_angle") - F.col("delta_next_angle"))) \
            .withColumn("angle_diff", F.when(F.col("angle_diff") > np.pi, 2 * np.pi - F.col("angle_diff")).otherwise(F.col("angle_diff"))) \
            .withColumn("unit_angle_diff", F.col("angle_diff") / np.pi) \
            .withColumn("bisector_magnitude", F.col("unit_angle_diff")) \
            .withColumn("bisector_magnitude", F.pow(F.col("bisector_magnitude"), 2)) \
            .withColumn("bisector_magnitude", 1 - F.col("bisector_magnitude")) \
            .withColumn("bisector_magnitude", 2 * F.col("bisector_magnitude")) \
            .withColumn("bisector_magnitude", F.lit(1.0)) \
            
            
        bisectors = bisectors \
            .withColumn("bisector_x", F.col("bisector_x") * F.col("bisector_magnitude")) \
            .withColumn("bisector_y", F.col("bisector_y") * F.col("bisector_magnitude")) \

        return bisectors \
            .select("animation_id", "effector_index", "frame_id", "time", "bisector_x", "bisector_y") \
            .orderBy("animation_id", "effector_index", "time") \
            
    def compute_knots(self, frames, peaks):
        """
        Compute splines based on curvature data and peaks.

        Parameters:
            frames (DataFrame): DataFrame containing frame data.
            peaks (DataFrame): DataFrame containing peak data.

        Returns:
            DataFrame: DataFrame with computed splines.
        """
        
        # Define a upper and lower bound for the spline
        lower_bound_peak_type = "positive"
        upper_bound_peak_type = "negative"

        # Define a window specification to partition by animation_id and effector_index
        window_spec = Window.partitionBy("animation_id", "effector_index").orderBy("time")

        print("peaks:")
        peaks.show()

        # find the first row in the window that is a peak and that matches the lower bound peak type
        # we flag this one as the lower bound of the spline
        lower_bounds = peaks \
            .filter(F.col("peak_type") == lower_bound_peak_type) \
            .withColumn("row_number", F.row_number().over(window_spec)) \
            .filter(F.col("row_number") == 1) \
            .drop("row_number") \
            .withColumn("lower_bound_time", F.col("time")) \
        
        print("lower_bounds:")
        lower_bounds.show()

        pruned_peaks = peaks \
            .join(lower_bounds.select("animation_id", "effector_index", "lower_bound_time"), ["animation_id", "effector_index"], "left") \
            .filter(F.col("time") >= F.col("lower_bound_time")) \
            
        print("pruned_peaks:")
        pruned_peaks.show()

        upper_bounds = pruned_peaks \
            .filter(F.col("peak_type") == upper_bound_peak_type) \
            .withColumn("row_number", F.row_number().over(window_spec)) \
            .filter(F.col("row_number") == 1) \
            .drop("row_number") \
            .withColumn("upper_bound_time", F.col("time")) \
            
        print("upper_bounds:")
        upper_bounds.show()
            
        full_frames = frames \
            .join(peaks.select("animation_id", "effector_index", "frame_id", "peak_type"), ["animation_id", "effector_index", "frame_id"], "left") \
            .join(lower_bounds.select("animation_id", "effector_index", "lower_bound_time"), ["animation_id", "effector_index"], "left") \
            .join(upper_bounds.select("animation_id", "effector_index", "upper_bound_time"), ["animation_id", "effector_index"], "left") \
            .orderBy("animation_id", "effector_index", "time") \
            
        print("full_frames:")
        full_frames.show()
            
        knots_with_time = full_frames \
            .filter(F.col("time") > F.col("lower_bound_time")) \
            .filter(F.col("time") < F.col("upper_bound_time")) \
            .withColumn("knot_index", F.row_number().over(window_spec)) \
            .withColumn("knot_index", F.col("knot_index") - 1) \
            .select("animation_id", "effector_index", "frame_id", "time", "knot_index") \
            
        print("knots_with_time:")
        knots_with_time.show()
            
        return knots_with_time
        

    
    def run(self, effectors, frames):
        """
        Compute curvatures for the given DataFrame.

        Parameters:
            effectors (DataFrame): DataFrame containing the effectors.

        Returns:
            DataFrame: DataFrame with computed curvatures.
        """
        curvatures_with_time = self.compute_curvatures(effectors, frames)
        peaks_with_time = self.compute_peaks(curvatures_with_time)
        knots_with_time = self.compute_knots(curvatures_with_time, peaks_with_time)
        tangents_with_time = self.compute_tangents(effectors, frames)
        bisectors_with_time = self.compute_bisectors(effectors, frames)

        curvatures = curvatures_with_time.drop("time")
        peaks = peaks_with_time.drop("time")
        tangents = tangents_with_time.drop("time")
        bisectors = bisectors_with_time.drop("time")
        knots = knots_with_time.drop("time")

        return curvatures, peaks, tangents, bisectors, knots


class SplineApp:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("SplineGenerator") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        self.generator = SplineGenerator(self.spark)

    def run(self, args):
        # Read the input CSV
        effectors = self.generator.read_csv(args.in_animation_effectors_csv_filepath)
        frames = self.generator.read_csv(args.in_animation_frames_csv_filepath)

        # Compute curvatures
        curvatures, peaks, tangents, bisectors, knots = self.generator.run(effectors, frames)

        # Write the output CSV
        self.generator.write_csv(curvatures, args.out_curvatures_csv_filepath)
        self.generator.write_csv(peaks, args.out_peaks_csv_filepath)
        self.generator.write_csv(tangents, args.out_tangents_csv_filepath)
        self.generator.write_csv(bisectors, args.out_bisectors_csv_filepath)
        self.generator.write_csv(knots, args.out_knots_csv_filepath)



def main():
    parser = argparse.ArgumentParser(description="Generate spline curvatures.")

    parser.add_argument("--in_animation_effectors_csv_filepath",
                        type=str,
                        default="/data/generated/animation_effectors.csv",
                        help="Path to the effectors input CSV file")
    parser.add_argument("--in_animation_frames_csv_filepath",
                        type=str,
                        default="/data/generated/animation_frames.csv",
                        help="Path to the frames input CSV file")
    parser.add_argument("--out_curvatures_csv_filepath",
                        type=str,
                        default="/data/generated/path_curvatures.csv",
                        help="Path to the output curvatures CSV file")
    parser.add_argument("--out_peaks_csv_filepath",
                        type=str,
                        default="/data/generated/path_curvature_peaks.csv",
                        help="Path to the output peaks CSV file")
    parser.add_argument("--out_tangents_csv_filepath",
                        type=str,
                        default="/data/generated/path_tangents.csv",
                        help="Path to the output tangents CSV file")
    parser.add_argument("--out_bisectors_csv_filepath",
                        type=str,
                        default="/data/generated/path_bisectors.csv",
                        help="Path to the output bisectors CSV file")
    parser.add_argument("--out_knots_csv_filepath",
                        type=str,
                        default="/data/generated/spline_knots.csv",
                        help="Path to the output spline knots CSV file")

    args = parser.parse_args()
    app = SplineApp()
    app.run(args)


if __name__ == "__main__":
    main()