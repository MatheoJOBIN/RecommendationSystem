import os
import json
import exifread
from PIL import Image
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Initialiser SparkSession
spark = SparkSession.builder \
    .appName("Image Metadata Processor") \
    .getOrCreate()

def extract_exif(filename):
    """
    Extracts EXIF data from the given image file.

    Args:
        filename (str): The path to the image file.

    Returns:
        dict: A dictionary containing the extracted EXIF data.
    """
    with open(filename, 'rb') as f:
        tags = exifread.process_file(f)
        return {str(tag): str(tags[tag]) for tag in tags}

def get_image_orientation(image_path):
    """
    Get the orientation of the image based on its width and height.

    Args:
        image_path (str): The path to the image file.

    Returns:
        str: The orientation of the image ('portrait' or 'landscape').
    """
    with Image.open(image_path) as img:
        width, height = img.size
        if width > height:
            return 'landscape'
        elif width < height:
            return 'portrait'
        else:
            return 'square'

# Registering UDFs for Spark DataFrame
extract_exif_udf = udf(extract_exif, StringType())
get_image_orientation_udf = udf(get_image_orientation, StringType())

# Define the path to the images folder and datas folder
images_directory = "images"
datas_directory = "datas"

# Load images into Spark DataFrame
image_files = [os.path.join(images_directory, filename) for filename in os.listdir(images_directory)]
df = spark.createDataFrame(image_files, StringType()).toDF("image_path")

# Extract EXIF data
df = df.withColumn("exif_data", extract_exif_udf(df["image_path"]))

# Get image orientation
df = df.withColumn("orientation", get_image_orientation_udf(df["image_path"]))

# Save data to JSON files
def save_to_json(row):
    patisserie_name = os.path.splitext(os.path.basename(row["image_path"]))[0]
    filepath = os.path.join(datas_directory, f"{patisserie_name}.json")
    
    with open(filepath, 'r') as f:
        existing_data = json.load(f)
    
    existing_data["exif"].update(json.loads(row["exif_data"]))
    existing_data["metadata"]["orientation"] = row["orientation"]
    
    with open(filepath, 'w') as f:
        json.dump(existing_data, f, indent=4)

df.foreach(save_to_json)

# Stop SparkSession
spark.stop()
