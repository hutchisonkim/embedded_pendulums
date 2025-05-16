import os
import shutil
from pyspark.sql import SparkSession

class BaseGenerator:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        
    def write_json(self, df, final_output_path: str):
        temp_output_path = final_output_path.replace(".json", "_json_temp")

        df.coalesce(1).write.json(temp_output_path)

        for file in os.listdir(temp_output_path):
            if file.startswith("part-") and file.endswith(".json"):
                if os.path.exists(final_output_path):
                    os.remove(final_output_path)
                shutil.move(os.path.join(temp_output_path, file), final_output_path)
                break

        shutil.rmtree(temp_output_path)

    def write_csv(self, df, final_output_path: str):
        temp_output_path = final_output_path.replace(".csv", "_csv_temp")

        df.coalesce(1).write.csv(temp_output_path, header=True)

        for file in os.listdir(temp_output_path):
            if file.startswith("part-") and file.endswith(".csv"):
                if os.path.exists(final_output_path):
                    os.remove(final_output_path)
                shutil.move(os.path.join(temp_output_path, file), final_output_path)
                break

        shutil.rmtree(temp_output_path)

    def read_csv(self, file_path: str, schema=None):
        if schema:
            return self.spark.read.csv(file_path, header=True, schema=schema)
        else:
            return self.spark.read.csv(file_path, header=True, inferSchema=True)
        
    def read_json(self, file_path: str):
        return self.spark.read.json(file_path)
    
    def write_py(self, content: str, file_path: str):
        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)
        with open(file_path, "w") as file:
            file.write(content)