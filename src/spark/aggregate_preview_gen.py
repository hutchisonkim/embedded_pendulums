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

class AggregatePreviewGen(BaseGenerator):

    def run(self, assets, peaks, output_dir):

        # Create the output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        # Extract unique animation IDs from the peaks DataFrame
        animation_ids = peaks.select("animation_id").distinct().collect()
        animation_ids = [row["animation_id"] for row in animation_ids]


        for animation_id in animation_ids:
            final_image = None
            #initialize the final image to a size that is large enough to hold all the images
            # for example, if each image is 100x100 and there are 4 images, the final image should be 200x200
            final_image_width = 0
            final_image_height = 0
            images = []
            for asset in assets:
                # load the image
                image_path = os.path.join(output_dir, f"{asset}_preview__{animation_id}.png")
                if os.path.exists(image_path):
                    print(f"Loading image for animation_id {animation_id} from {image_path}")
                    # load the image and convert RGBA to RGB if necessary
                    image = plt.imread(image_path)
                    images.append(image)
                    # update the final image size
                    final_image_width = max(final_image_width, image.shape[1])
                    final_image_height += image.shape[0]

            if images:
                # Initialize the final image with appropriate size and RGBA channels
                final_image = np.zeros((final_image_height, final_image_width, 4), dtype=np.uint8)
                y_offset = 0

                for image in images:
                    # Convert to uint8 if necessary
                    if image.dtype != np.uint8:
                        image = (image * 255).astype(np.uint8)

                    # Handle images with alpha channel
                    if image.shape[-1] == 4:
                        image = image[:, :, :3]  # Discard alpha channel

                    height, width, _ = image.shape
                    #non-centered horizontally:
                    # final_image[y_offset:y_offset + height, :width, :3] = image
                    #centered horizontally:
                    x_offset = (final_image_width - width) // 2
                    final_image[y_offset:y_offset + height, x_offset:x_offset + width, :3] = image
                    y_offset += height

                # Set alpha channel to fully opaque
                final_image[:, :, 3] = 255

                # Debug final image properties
                print(f"Final image shape: {final_image.shape}, dtype: {final_image.dtype}")
                print(f"Pixel range: {final_image.min()} - {final_image.max()}")

                # Save the final image, scaling if necessary
                final_image_path = os.path.join(output_dir, f"aggregate_preview__{animation_id}.png")
                plt.imsave(final_image_path, final_image)
                print(f"Saved aggregate preview for animation_id {animation_id} at {final_image_path}")

class AggregatePreviewGenApp:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("AggregatePreviewGenApp") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        self.generator = AggregatePreviewGen(self.spark)

    def run(self, args):
        # Extract
        assets = args.assets
        peaks = self.generator.read_csv(args.in_path_curvature_peaks_csv_filepath)

        # Transform & Load
        self.generator.run(assets, peaks, args.out_previews_dir)

def main():
    parser = argparse.ArgumentParser(description="Generate curvature previews.")

    parser.add_argument("--assets",
                        nargs="+",
                        default=["animation", "bisector", "spline"],
                        help="List of assets (e.g., animation bisector)")
    parser.add_argument("--in_path_curvature_peaks_csv_filepath",
                        type=str,
                        default="/data/generated/path_curvature_peaks.csv",
                        help="Path to the peaks input CSV file")
    parser.add_argument("--out_previews_dir",
                        type=str,
                        default="/data/generated/previews",
                        help="Directory to save the curvature previews")

    args = parser.parse_args()
    app = AggregatePreviewGenApp()
    app.run(args)

if __name__ == "__main__":
    main()
