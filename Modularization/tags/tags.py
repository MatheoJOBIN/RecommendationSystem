import os
import random
from PIL import Image
import matplotlib.pyplot as plt
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialiser SparkSession
spark = SparkSession.builder \
    .appName("Image Tagging") \
    .getOrCreate()

# Récupérer la liste des images
image_dir = "images"
image_files = [f for f in os.listdir(image_dir) if os.path.isfile(os.path.join(image_dir, f)) and f.lower().endswith(('.png', '.jpg', '.jpeg'))]

# Choisir 5 images au hasard
selected_images = random.sample(image_files, 5)

# Define the data directory
data_dir = "datas"

def process_image(image_file):
    # Afficher l'image
    img = Image.open(os.path.join(image_dir, image_file))
    plt.imshow(img)
    plt.show(block=False)
    time.sleep(1)  # Pause pour donner le temps à l'image de s'afficher

    # Demander à l'utilisateur d'entrer un mot pour qualifier l'image
    tag = input("Entrez un mot pour qualifier cette image: ")

    # Lecture du fichier JSON existant dans le data directory
    json_file = os.path.join(data_dir, os.path.splitext(image_file)[0] + ".json")
    if os.path.exists(json_file):
        try:
            with open(json_file, 'r') as file:
                image_data = json.load(file)
        except json.JSONDecodeError:
            print(f"Error decoding JSON for {image_file}. Skipping this file.")
            image_data = {}
    else:
        print(f"No JSON file found for {image_file} in the data directory.")
        image_data = {}

    # Ajout du tag dans la balise "tags"
    if 'tags' not in image_data:
        image_data['tags'] = {}
    elif not isinstance(image_data['tags'], dict):
        # Si c'est une liste, convertir en dictionnaire
        tag_list = image_data['tags']
        image_data['tags'] = {tag: 0 for tag in tag_list}
    # Incrémenter le nombre d'occurrences du tag
    image_data['tags'][tag] = image_data['tags'].get(tag, 0) + 1

    # Écriture dans le fichier JSON in the data directory
    with open(json_file, 'w', encoding='utf-8') as file:
        json.dump(image_data, file, ensure_ascii=False, indent=4)

    # Fermer l'image
    plt.close()

# Parallelize the selected images and apply the processing function
spark.sparkContext.parallelize(selected_images).foreach(process_image)

# Stop SparkSession
spark.stop()
