import os
import sys
import json
import shutil
import urllib.parse
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import requests

# Initialiser SparkSession
spark = SparkSession.builder \
    .appName("Wikidata Image Downloader") \
    .getOrCreate()


def download_image(url, directory):
    """
    Download an image from the given URL and save it to the specified directory.

    Args:
        url (str): The URL of the image to download.
        directory (str): The directory to save the downloaded image.

    Returns:
        int: The status code of the HTTP request.

    """
    headers = {"User-Agent": "Mozilla/5.0"}
    request = requests.get(url, allow_redirects=True, headers=headers, stream=True)
    if request.status_code == 200:
        filename = os.path.join(directory, os.path.basename(url))  # Get filename from URL
        # Decode the URL encoded filename to get readable text
        filename = urllib.parse.unquote(filename)
        with open(filename, "wb") as image:
            request.raw.decode_content = True
            shutil.copyfileobj(request.raw, image)
        return filename
    else: 
        return None

def create_json_file(image_filename, datas_directory, country):
    """
    Create a JSON file from the given pandas dataframe.

    Args:
        image_filename (str): The filename of the image.
        datas_directory (str): The directory to save the JSON file.
        country (str): The country of origin.

    """
    filename = os.path.splitext(os.path.basename(image_filename))[0]
    formatted_data = {
        "exif": {},
        "metadata": {"originCountry": country},
        "tags": {}
    }
    # Save the formatted data to a JSON file
    filepath = os.path.join(datas_directory, f"{filename}.json")
    with open(filepath, 'w') as f:
        json.dump(formatted_data, f, indent=4)
     

def get_results(endpoint_url, query):
    """
    Execute a SPARQL query on the given endpoint URL and return the results.

    Args:
        endpoint_url (str): The URL of the SPARQL endpoint.
        query (str): The SPARQL query to execute.

    Returns:
        dict: The JSON-formatted results of the SPARQL query.

    """
    user_agent = "WDQS-example Python/%s.%s" % (
        sys.version_info[0],
        sys.version_info[1],
    )
    sparql = SPARQLWrapper(endpoint_url, agent=user_agent)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    return sparql.query().convert()

# SPARQL query
query = """
SELECT DISTINCT ?patisserie ?patisserieLabel ?image ?country ?countryLabel {
  ?patisserie wdt:P31*/wdt:P279* wd:Q477248;
          wdt:P18 ?image;
          wdt:P495 ?country.
 SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
}
LIMIT 10
"""

# Wikidata SPARQL endpoint
wikidata_endpoint = "https://query.wikidata.org/sparql"

images_directory = "images" 
datas_directory = "datas"

results = get_results(wikidata_endpoint, query)

# Convert results to Spark DataFrame
df = spark.createDataFrame(results["results"]["bindings"])

# Download images using Spark DataFrame
download_image_udf = spark.udf.register("download_image", download_image)
df = df.withColumn("image_filename", download_image_udf(df["image"], lit(images_directory)))

# Create JSON files using Spark DataFrame
create_json_file_udf = spark.udf.register("create_json_file", lambda image_filename, country, datas_directory: create_json_file(image_filename, datas_directory, country))
df.foreachPartition(lambda partition: 
    [create_json_file_udf(row["image_filename"], row["country"], lit(datas_directory)) for row in partition])

# Stop SparkSession
spark.stop()