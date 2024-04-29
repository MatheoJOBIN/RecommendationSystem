import os
import json
import matplotlib.pyplot as plt
from collections import Counter
from IPython.display import display
from PIL import Image
import numpy as np
from pyspark.sql import SparkSession

# Initialiser SparkSession
spark = SparkSession.builder \
    .appName("Image Analysis") \
    .getOrCreate()

# Répertoire des données
datas_directory = "datas"

# Chemins des images favorites
paths = [...]

def process_image(path):
    # 1. Affichage des images favorites
    img = Image.open(path)
    display(img)

    # 2. Détermination de l'orientation d'image favorite
    image_name = os.path.splitext(os.path.basename(path))[0]
    with open(os.path.join(datas_directory, f"{image_name}.json"), 'r') as f:
        image_data = json.load(f)
    orientation = image_data["metadata"]["orientation"]
    
    return (orientation, path)

# Traitement des images en parallèle
image_rdd = spark.sparkContext.parallelize(paths)
orientation_paths_rdd = image_rdd.map(process_image)

# 3. Création d'un graphique en camembert pour montrer la répartition des orientations d'images
orientation_counts = orientation_paths_rdd.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x + y).collect()
labels = [orientation.capitalize() for orientation, _ in orientation_counts]
sizes = [count for _, count in orientation_counts]

plt.figure(figsize=(8, 6))
plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140)
plt.title("Répartition des orientations d'images")
plt.show()

# 4. Analyse des couleurs préférées de l'utilisateur
def extract_dominant_colors(path):
    image = Image.open(path)
    # Extraction des couleurs dominantes de l'image
    dominant_colors = []
    for path in paths:
        image = Image.open(path)
        dominant_colors.extend(extract_dominant_colors(image))
        return dominant_colors

dominant_colors_rdd = image_rdd.flatMap(extract_dominant_colors)
color_counts = dominant_colors_rdd.map(lambda x: (tuple(x), 1)).reduceByKey(lambda x, y: x + y).collect()

# Afficher les couleurs préférées de l'utilisateur
for color, _ in color_counts[:3]:
    plt.figure(figsize=(2, 2))
    plt.imshow([[np.array(color) / 255]])
    plt.axis('off')
    plt.show()

# 5. Détermination des tags les plus fréquents
def process_tags(path):
    image_name = os.path.splitext(os.path.basename(path))[0]
    with open(os.path.join(datas_directory, f"{image_name}.json"), 'r') as f:
        image_data = json.load(f)
    tags = list(image_data.get('tags', {}).keys())
    return tags

tags_rdd = image_rdd.flatMap(process_tags)
tag_counts = tags_rdd.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).collect()

# Afficher les tags les plus fréquents pour les images favorites
tag_names = [tag for tag, _ in tag_counts[:3]]
tag_frequencies = [freq for _, freq in tag_counts[:3]]

plt.figure(figsize=(8, 6))
plt.bar(tag_names, tag_frequencies, color='skyblue')
plt.xlabel('Tags')
plt.ylabel('Fréquence')
plt.title('Tags les plus fréquents pour les images favorites')
plt.xticks(rotation=45)
plt.show()

# Stop SparkSession
spark.stop()
