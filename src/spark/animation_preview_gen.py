import os
import argparse
import matplotlib.pyplot as plt
import colorsys
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, struct, explode
from spark.base_gen import BaseGenerator

class AnimationPreviewGenerator(BaseGenerator):
    def draw_animation(self, frames, output_path):
        """
        Draws the first animation in the dataset.

        Parameters:
        - frames: list, the frames of the animation.
        - output_path: str, the path to save the output image.
        """
        # Create the output directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Initialize the figure with a black background
        fig, ax = plt.subplots(figsize=(8, 8))
        ax.axis("equal")
        ax.axis("off")
        fig.patch.set_facecolor("black")  # Set the figure background to black
        ax.set_facecolor("black")        # Set the axes background to black

        # Normalize alpha through time
        num_frames = len(frames)
        for frame_index, frame in enumerate(frames):
            sat = 0.1 + (frame_index / num_frames) * 0.9
            val = sat
            effectors = frame["effectors"]

            # Extract effector positions
            positions = np.array([[effector["position_x"], effector["position_y"]] for effector in effectors])

            # Plot lines connecting effectors
            for i in range(len(positions) - 1):
                hue = 0.6 + (0.4 * i / len(positions))  # Gradual hue change
                alpha = sat
                color = colorsys.hsv_to_rgb(hue, sat, val)
                ax.plot(
                    [positions[i][0], positions[i + 1][0]],
                    [positions[i][1], positions[i + 1][1]],
                    "-", color=color, alpha=alpha
                )

        # Save the figure
        plt.tight_layout()
        plt.savefig(output_path, dpi=300, facecolor=fig.get_facecolor())  # Ensure the black background is saved
        plt.close(fig)

    def generate_previews(self, animations_df, output_dir):
        animations_df = animations_df.select(explode(col("animations")).alias("animation")).select(
            col("animation.id").alias("id"),
            col("animation.frames").alias("frames")
        )
        animations = animations_df.collect()
        for animation in animations:
            animation_id = animation["id"]
            frames = animation["frames"]
            output_path = os.path.join(output_dir, f"animation_preview__{animation_id}.png")
            self.draw_animation(frames, output_path)
    
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


        array = effector_frames_array
        lines = array["lines"]
        num_lines = len(lines)
        highest_saturation = 0.0
        for line_index, line in enumerate(lines):
            hue = 0.6 + (line_index / (num_lines - 1)) * 0.4 # hue based on effector index

            points = line["points"]
            num_points = len(points)
            for i in range(num_points - 1):
                sat = 0.1 + (i / (num_points - 2)) * 0.75 # sat based on frame index
                val = sat
                alpha = sat
                sat = 0.0
                color = colorsys.hsv_to_rgb(hue, sat, val)
                ax.plot(
                    [points[i]["x"], points[i + 1]["x"]],
                    [points[i]["y"], points[i + 1]["y"]],
                    "--", color=color, alpha=alpha
                )
                highest_saturation = max(highest_saturation, sat)


        print("highest_saturation b:")
        print(highest_saturation)

        plt.tight_layout()
        plt.savefig(output_filepath, dpi=300, facecolor=fig.get_facecolor())  # Ensure the black background is saved
        plt.close(fig)


    def generate_previews2(self, frames, effectors, output_dir):

        print("effectors:")
        effectors.show()

        effectors = effectors \
            .withColumnRenamed("effector_position_x", "x") \
            .withColumnRenamed("effector_position_y", "y") \
            .join(frames.select("animation_id", "frame_id", "time"), ["animation_id", "frame_id"], "left") \
            
        print("effectors:")
        effectors.show()

        effector_frames = effectors \
            .orderBy("animation_id", "effector_index", "time") \
            .groupBy("animation_id", "effector_index") \
            .agg(collect_list(struct("x", "y")).alias("points")) \
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
            .agg(collect_list(struct("x", "y")).alias("points")) \
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
                                       f"animation_preview__{frame_effectors_array['animation_id']}.png")
        self._draw_spline(effector_frames_array, frame_effectors_array, output_filepath)

class AnimationPreviewApp:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("AnimationPreviewGen") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        self.generator = AnimationPreviewGenerator(self.spark)

    def run(self, args):
        # Extract
        frames = self.generator.read_csv(args.in_animation_frames_csv_filepath)
        effectors = self.generator.read_csv(args.in_animation_effectors_csv_filepath)

        # Transform & Load
        self.generator.generate_previews2(frames, effectors, args.out_previews_dir)



def main():
    parser = argparse.ArgumentParser(description="Generate animation previews.")

    parser.add_argument("--in_animation_effectors_csv_filepath",
                        type=str,
                        default="/data/generated/animation_effectors.csv",
                        help="Path to the effectors input CSV file")
    parser.add_argument("--in_animation_frames_csv_filepath",
                        type=str,
                        default="/data/generated/animation_frames.csv",
                        help="Path to the frames input CSV file")
    parser.add_argument("--out_previews_dir",
                        type=str,
                        default="/data/generated/previews",
                        help="Directory to save the animation previews")

    args = parser.parse_args()
    app = AnimationPreviewApp()
    app.run(args)

if __name__ == "__main__":
    main()

    