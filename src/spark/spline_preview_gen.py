import os
import argparse
import matplotlib.pyplot as plt
import colorsys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, struct, when
from pyspark.sql import functions as F
from spark.base_gen import BaseGenerator

class SplinePreviewGenerator(BaseGenerator):
    def _draw_spline(self, effector_frames_array, frame_effectors_array, output_filepath):
        # Create the output directory if it doesn't exist
        os.makedirs(os.path.dirname(output_filepath), exist_ok=True)

        # Initialize the figure with a black background
        fig, ax = plt.subplots(figsize=(8, 8))
        ax.axis("equal")
        ax.axis("off")
        fig.patch.set_facecolor("black")  # Set the figure background to black
        ax.set_facecolor("black")        # Set the axes background to black


        array = frame_effectors_array
        lines = array["lines"]
        num_lines = len(lines)
        highest_saturation = 0.0
        for line_index, line in enumerate(lines):
            sat = 0.1 + (line_index / (num_lines - 1)) * 0.9 # sat based on frame index
            val = sat

            sat = 0.1 + (line_index / (num_lines - 1)) * 0.4 # sat based on frame index
            val = sat

            points = line["points"]
            num_points = len(points)
            for i in range(num_points - 1):
                hue = 0.6 + (i / (num_points - 2)) * 0.25 # hue based on effector index
                alpha = sat
                color = colorsys.hsv_to_rgb(hue, sat, val)
                ax.plot(
                    [points[i]["x"], points[i + 1]["x"]],
                    [points[i]["y"], points[i + 1]["y"]],
                    "-", color=color, alpha=alpha
                )
                highest_saturation = max(highest_saturation, sat)
        print("highest_saturation a:")
        print(highest_saturation)

        lower_bound = "positive"
        upper_bound = "negative"

        array = effector_frames_array
        lines = array["lines"]
        num_lines = len(lines)
        highest_saturation = 0.0
        for line_index, line in enumerate(lines):
            hue = 0.6 + (line_index / (num_lines - 1)) * 0.4 # hue based on effector index

            is_after_crossover = False
            is_after_positive = False
            points = line["points"]
            num_points = len(points)
            for i in range(num_points - 1):
                sat = 0.25 + (i / (num_points - 1)) * 0.75 # sat based on frame index
                val = sat
                alpha = sat

                alpha = 1.0
                if not is_after_crossover:
                    alpha = 0.1
                if is_after_positive:
                    alpha *= 0.1

                sat = 0.0

                color = colorsys.hsv_to_rgb(hue, sat, val)
                ax.plot(
                    [points[i]["x"], points[i + 1]["x"]],
                    [points[i]["y"], points[i + 1]["y"]],
                    "-", color=color, alpha=alpha
                )
                if points[i + 1]["peak_type"] == upper_bound or points[i]["peak_type"] == upper_bound:
                    if not is_after_positive:
                        is_after_positive = True
                if points[i + 1]["peak_type"] == lower_bound or points[i]["peak_type"] == lower_bound:
                    if not is_after_crossover:
                        is_after_crossover = True

            is_after_crossover = False
            is_after_positive = False
            for i in range(num_points):
                sat = 0.25 + (i / (num_points - 1)) * 0.75 # sat based on frame index
                val = sat
                alpha = sat

                sat = 0.0

                if points[i]["peak_type"] == lower_bound:
                    if not is_after_crossover:
                        is_after_crossover = True


                # alpha = 1.0
                factor = 1.0
                alt_factor = 0.1
                if not is_after_crossover:
                    factor = alt_factor
                if is_after_positive:
                    factor = alt_factor
                alpha *= factor

                    
                color = colorsys.hsv_to_rgb(hue, sat, val)
                if points[i]["peak_type"] is not None:
                    ax.plot(
                        points[i]["x"], points[i]["y"],
                        "o", color=color, alpha=alpha, markersize=5
                    )
                # else:
                #     ax.plot(
                #         points[i]["x"], points[i]["y"],
                #         "o", color=color, alpha=alpha, markersize=3
                #     )
                
                #draw bisectors
                if points[i]["bisector_x"] is not None and points[i]["bisector_y"] is not None and points[i]["peak_type"] is not None:
                    if points[i]["peak_type"] == "crossover":
                        # circle = plt.Circle((points[i]["x"], points[i]["y"]), 0.025, color=color, fill=False, alpha=alpha, linewidth=1)
                        # ax.add_artist(circle)
                        
                        ax.plot(
                            points[i]["x"], points[i]["y"],
                            "o", color="black", alpha=1.0, markersize=3
                        )
                    else:
                        ax.plot(
                            [points[i]["x"], points[i]["x"] - points[i]["bisector_x"] * 0.2 * points[i]["curvature"]],
                            [points[i]["y"], points[i]["y"] - points[i]["bisector_y"] * 0.2 * points[i]["curvature"]],
                            "-", color=color, alpha=alpha, linewidth=1
                        )
                        ax.plot(
                            points[i]["x"], points[i]["y"],
                            "o", color="black", alpha=1.0, markersize=3
                        )
                        ax.plot(
                            points[i]["x"] - points[i]["bisector_x"] * 0.2 * points[i]["curvature"],
                            points[i]["y"] - points[i]["bisector_y"] * 0.2 * points[i]["curvature"],
                            "o", color=color, alpha=alpha, markersize=3
                        )
                        # circle = plt.Circle((points[i]["x"], points[i]["y"]), 0.025, color=color, fill=False, alpha=alpha, linewidth=1)
                        # ax.add_artist(circle)

                if points[i]["peak_type"] == upper_bound:
                    if not is_after_positive:
                        is_after_positive = True

                highest_saturation = max(highest_saturation, sat)


        print("highest_saturation b:")
        print(highest_saturation)




        # Save the figure
        plt.tight_layout()
        plt.savefig(output_filepath, dpi=300, facecolor=fig.get_facecolor())  # Ensure the black background is saved
        plt.close(fig)

    def generate_previews_with_peaks(self, frames, effectors, curvatures, peaks, tangents, bisectors, output_dir):
                  
        print("effectors:")
        effectors.show()

        effectors = effectors \
            .withColumnRenamed("effector_position_x", "x") \
            .withColumnRenamed("effector_position_y", "y") \
            .join(frames.select("animation_id", "frame_id", "time"), ["animation_id", "frame_id"], "left") \
            .join(curvatures.select("animation_id", "frame_id", "effector_index", "curvature"), ["animation_id", "frame_id", "effector_index"], "left") \
            .join(peaks, ["animation_id", "frame_id", "effector_index"], "left") \
            .join(tangents, ["animation_id", "frame_id", "effector_index"], "left") \
            .join(bisectors, ["animation_id", "frame_id", "effector_index"], "left") \

        print("effectors:")
        effectors.show()

        max_abs_curvatures = effectors \
            .select("animation_id", "frame_id", "effector_index", "curvature") \
            .withColumn("abs_curvature", F.abs(col("curvature"))) \
            .groupBy("animation_id", "effector_index") \
            .agg(F.max("abs_curvature").alias("max_abs_curvature")) \

        print("max_abs_curvatures:")
        max_abs_curvatures.show()

        effectors = effectors \
            .join(max_abs_curvatures, ["animation_id", "effector_index"], "left") \

        print("effectors:")
        effectors.show()

        effectors = effectors \
            .withColumn("curvature", when(col("max_abs_curvature") != 0, F.abs(col("curvature")) / col("max_abs_curvature")).otherwise(0)) \
            
        effectors = effectors \
            .withColumn("curvature", F.lit(1.0)) \

        print("effectors:")
        effectors.show()

        effector_frames = effectors \
            .orderBy("animation_id", "effector_index", "time") \
            .groupBy("animation_id", "effector_index") \
            .agg(collect_list(struct("x", "y", "peak_type", "tangent", "curvature", "bisector_x", "bisector_y")).alias("points")) \
            .groupBy("animation_id") \
            .agg(collect_list(struct("effector_index", "points")).alias("lines")) \
            
        print("effector_frames:")
        effector_frames.show()
            
        effector_frames_array = effector_frames \
            .rdd.map(lambda row: {
                "animation_id": row["animation_id"],
                "lines": [line.asDict() for line in row["lines"]]
            }).collect()
        
        # take just the first animation_id for preview
        effector_frames_array = effector_frames_array[0]
        
        print("effector_frames_array:")
        print(effector_frames_array)

        frame_effectors = effectors \
            .groupBy("animation_id", "frame_id") \
            .agg(collect_list(struct("x", "y", "peak_type", "tangent", "curvature", "bisector_x", "bisector_y")).alias("points")) \
            .join(frames.select("animation_id", "frame_id", "time"), ["animation_id", "frame_id"], "left") \
            .orderBy("animation_id", "time") \
            .drop("frame_id") \
            .groupBy("animation_id") \
            .agg(collect_list(struct("time", "points")).alias("lines")) \
            
        print("frame_effectors:")
        frame_effectors.show()

        frame_effectors_array = frame_effectors \
            .rdd.map(lambda row: {
                "animation_id": row["animation_id"],
                "lines": [line.asDict() for line in row["lines"]]
            }).collect()
                   
        # take just the first animation_id for preview
        frame_effectors_array = frame_effectors_array[0]

        print("frame_effectors_array:")
        print(frame_effectors_array)

        output_filepath = os.path.join(output_dir,
                                       f"spline_preview__{frame_effectors_array['animation_id']}.png")
        
        self._draw_spline(effector_frames_array, frame_effectors_array, output_filepath)


class SplinePreviewApp:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("SplinePreviewGen") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        self.generator = SplinePreviewGenerator(self.spark)

    def run(self, args):

        # Extract
        frames = self.generator.read_csv(args.in_animation_frames_csv_filepath)
        effectors = self.generator.read_csv(args.in_animation_effectors_csv_filepath)
        curvatures = self.generator.read_csv(args.in_path_curvatures_csv_filepath)
        peaks = self.generator.read_csv(args.in_path_curvature_peaks_csv_filepath)
        tangents = self.generator.read_csv(args.in_path_curvature_tangents_csv_filepath)
        bisectors = self.generator.read_csv(args.in_path_bisectors_csv_filepath)

        # Transform & Load
        self.generator.generate_previews_with_peaks(frames, effectors, curvatures, peaks, tangents, bisectors, args.out_previews_dir)

def main():
    parser = argparse.ArgumentParser(description="Generate spline previews with peaks.")

    parser.add_argument("--in_animation_effectors_csv_filepath",
                        type=str,
                        default="/data/generated/animation_effectors.csv",
                        help="Path to the effectors input CSV file")
    parser.add_argument("--in_animation_frames_csv_filepath",
                        type=str,
                        default="/data/generated/animation_frames.csv",
                        help="Path to the frames input CSV file")
    parser.add_argument("--in_path_curvatures_csv_filepath",
                        type=str,
                        default="/data/generated/path_curvatures.csv",
                        help="Path to the curvatures input CSV file")
    parser.add_argument("--in_path_curvature_peaks_csv_filepath",
                        type=str,
                        default="/data/generated/path_curvature_peaks.csv",
                        help="Path to the peaks input CSV file")
    parser.add_argument("--in_path_curvature_tangents_csv_filepath",
                        type=str,
                        default="/data/generated/path_tangents.csv",
                        help="Path to the tangents input CSV file")
    parser.add_argument("--in_path_bisectors_csv_filepath",
                        type=str,
                        default="/data/generated/path_bisectors.csv",
                        help="Path to the bisectors input CSV file")
    parser.add_argument("--out_previews_dir",
                        type=str,
                        default="/data/generated/previews",
                        help="Directory to save the spline previews")

    args = parser.parse_args()
    app = SplinePreviewApp()
    app.run(args)

if __name__ == "__main__":
    main()