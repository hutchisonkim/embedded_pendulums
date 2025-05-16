import os
import argparse
import matplotlib.pyplot as plt
import colorsys
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, struct, explode, lit, when, row_number
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from spark.base_gen import BaseGenerator
from matplotlib.collections import PolyCollection
from matplotlib.colors import Normalize, LinearSegmentedColormap

class CurvaturePreviewGen(BaseGenerator):


    def run(self, frames, curvatures, peaks, output_dir):

        curvatures_with_peaks = frames \
            .join(curvatures \
                  .select("animation_id", "frame_id", "effector_index", "curvature"),
                    ["animation_id", "frame_id"], "left") \
            .join(peaks \
                  .select("animation_id", "frame_id", "effector_index", "peak_type"),
                    ["animation_id", "frame_id", "effector_index"], "left") \
            .orderBy("animation_id", "effector_index", "time") \
            .groupBy("animation_id", "effector_index") \
            .agg(collect_list(struct("time", "curvature", "peak_type")).alias("points")) \
            
        print("curvatures_with_peaks:")
        curvatures_with_peaks.show()

        curvatures_with_peaks_array = curvatures_with_peaks \
            .rdd \
            .map(lambda row: {
                "animation_id": row["animation_id"],
                "effector_index": row["effector_index"],
                "points": [
                    {
                        "time": point["time"],
                        "curvature": point["curvature"],
                        "peak_type": point["peak_type"]
                    }
                    for point in row["points"]
                ]
            }) \
            .collect() \
        
        # take just the first animation_id for preview
        curvatures_with_peaks_elem = curvatures_with_peaks_array[0]

        print("curvatures_with_peaks_elem:")
        print(curvatures_with_peaks_elem)

        output_filepath = os.path.join(output_dir, f"curvature_preview__{curvatures_with_peaks_elem['animation_id']}.png")
        
        self._draw(curvatures_with_peaks_array, output_filepath)
            
                  
            
    def _draw(self, curves_json, output_filepath):

        # Create the output directory if it doesn't exist
        os.makedirs(os.path.dirname(output_filepath), exist_ok=True)

        # Initialize the figure with a black background
        fig, ax = plt.subplots(figsize=(10, 6))

        #make the bg color grayscale, 12% white
        bg_color = (0.12, 0.12, 0.12)  # RGB values for a light gray color
        ax.set_facecolor(bg_color)
        fig.patch.set_facecolor(bg_color)
        ax.axis("off")

        # Define a upper and lower bound for the spline
        lower_bound = "positive"
        upper_bound = "negative"
        

        # Normalize the number of curves for color mapping
        num_curves = len(curves_json)

        for curve_index, curve_json in enumerate(curves_json):
            
            # Generate a unique color for each curve using HSV
            hue = curve_index / num_curves
            hue = 0.6 + (0.4 * (curve_index) / (num_curves + 1))  # Gradual hue change
            hue -= 0.594
            color = colorsys.hsv_to_rgb(hue, 0.8, 0.8)
            bg_hue = hue

            # Extract time and curvature values
            points_json = curve_json["points"]
            times = [point_json["time"] for point_json in points_json]
            values = [point_json["curvature"] for point_json in points_json]
            

            # Fill the area under the curve
            alpha = 0.6
            val = 0.8

            alpha = 0.4
            val = 0.6
            color = colorsys.hsv_to_rgb(hue, 0.8, val)

            is_after_crossover = False
            is_after_positive = False

            for i in range(len(times) - 1):
                alpha = 0.5 + (i / (len(times) - 1)) * 0.5
                color = colorsys.hsv_to_rgb(bg_hue, 0.8, 1.0)
                alpha = 1

                if points_json[i]["peak_type"] == lower_bound:
                    if not is_after_crossover:
                        is_after_crossover = True

                alpha_factor = 1.0
                if not is_after_crossover:
                    alpha_factor *= 0.4
                if is_after_positive:
                    alpha_factor *= 0.4

                bg_alpha = 0.01 * alpha_factor
                line_alpha = 1.0 * alpha_factor

                # ax.fill_between([times[i], times[i + 1]], [values[i], values[i + 1]], color=color, alpha=bg_alpha, linewidth=0)
                ax.plot([times[i], times[i + 1]], [values[i], values[i + 1]], color=color, alpha=line_alpha, linewidth=1)

                if curve_index == 0:
                    alpha = 0.1 + (i / (len(times) - 1)) * 0.9
                    ax.plot(times[i], 0, ".", color="white", alpha=alpha, markersize=5, markeredgewidth=0)
                    if i == len(times) - 2:
                        ax.plot(times[i + 1], 0, ".", color="white", alpha=alpha, markersize=5, markeredgewidth=0)
                
                if points_json[i+1]["peak_type"] == upper_bound:
                    if not is_after_positive:
                        is_after_positive = True


        plt.tight_layout()
        plt.savefig(output_filepath, dpi=300, facecolor=fig.get_facecolor())  # Ensure the black background is saved
        plt.close(fig)


class CurvaturePreviewGenApp:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("CurvaturePreviewGenApp") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        self.generator = CurvaturePreviewGen(self.spark)

    def run(self, args):
        # Extract
        frames = self.generator.read_csv(args.in_animation_frames_csv_filepath)
        curvatures = self.generator.read_csv(args.in_path_curvatures_csv_filepath)
        peaks = self.generator.read_csv(args.in_path_curvature_peaks_csv_filepath)

        # Transform & Load
        self.generator.run(frames, curvatures, peaks, args.out_previews_dir)

def main():
    parser = argparse.ArgumentParser(description="Generate curvature previews.")

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
    parser.add_argument("--out_previews_dir",
                        type=str,
                        default="/data/generated/previews",
                        help="Directory to save the curvature previews")

    args = parser.parse_args()
    app = CurvaturePreviewGenApp()
    app.run(args)

if __name__ == "__main__":
    main()