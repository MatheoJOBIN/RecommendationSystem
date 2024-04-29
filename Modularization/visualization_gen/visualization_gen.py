import os
import json
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from collections import Counter

# Define the directory containing JSON files
datas_directory = "datas"

# Initialiser SparkSession
spark = SparkSession.builder \
    .appName("Tag Frequency Analyzer") \
    .getOrCreate()

# Répertoire des données
images_directory = "images"
datas_directory = "datas"

def process_json(json_file):
    # Charger les données JSON
    with open(json_file, 'r') as file:
        image_data = json.load(file)
    
    # Récupérer le nom de l'image à partir du nom du fichier JSON
    image_name = os.path.splitext(os.path.basename(json_file))[0]
    
    # Récupérer les tags de l'image
    tags = image_data.get('tags', {})
    
    # Initialisation d'un dictionnaire pour stocker les tags et leur fréquence
    tag_frequency = {}
    
    # Mettre à jour le dictionnaire de fréquence des tags
    for tag, frequency in tags.items():
        # Convertir le tag en chaîne de caractères s'il s'agit d'un dictionnaire
        if isinstance(tag, dict):
            tag = str(tag)
        tag_frequency[tag] = frequency
    
    return image_name, tag_frequency

# Charger tous les fichiers JSON dans le répertoire des données
json_files = [os.path.join(datas_directory, f) for f in os.listdir(datas_directory) if f.endswith('.json')]

# Traitement des fichiers JSON en parallèle
tag_frequency_rdd = spark.sparkContext.parallelize(json_files).map(process_json)

# Convertir RDD en DataFrame
tag_frequency_df = tag_frequency_rdd.toDF(["Image", "Tag_Frequency"])

# Affichage des graphiques en barres pour chaque image
def plot_tag_frequency(row):
    image_name = row["Image"]
    tag_frequency = row["Tag_Frequency"]
    
    plt.figure(figsize=(10, 6))
    plt.bar(tag_frequency.keys(), tag_frequency.values())
    plt.xlabel('Tags')
    plt.ylabel('Nombre de mentions')
    plt.title(f'Fréquence des tags pour {image_name}')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.show()

tag_frequency_df.foreach(plot_tag_frequency)

# Stop SparkSession
spark.stop()


# Initialiser un compteur pour chaque orientation
orientation_counter = Counter()

# Parcourir tous les fichiers JSON dans le répertoire des données
json_files = [f for f in os.listdir(datas_directory) if f.endswith('.json')]
for json_file in json_files:
    with open(os.path.join(datas_directory, json_file), 'r') as file:
        image_data = json.load(file)
        orientation = image_data["metadata"]["orientation"]
        orientation_counter[orientation] += 1

# Extraire les orientations et leurs fréquences
orientations, frequencies = zip(*orientation_counter.items())

# Créer un graphique en barres avec légende
plt.figure(figsize=(8, 6))
plt.bar(orientations, frequencies, color=['blue', 'green', 'red'])
plt.xlabel('Orientation')
plt.ylabel("Nombre d'images")
plt.title("Nombre d'images dans différentes orientations")
plt.legend(orientations)
plt.show()


def extract_camera_models_from_json(datas_directory):
    """
    Extracts camera models from JSON files in the specified directory.

    Args:
        datas_directory (str): The directory containing JSON files.

    Returns:
        list: A list of camera models extracted from the JSON files.
    """
    camera_models = []
    # Utiliser une compréhension de liste pour parcourir les fichiers JSON
    json_files = [f for f in os.listdir(datas_directory) if f.endswith('.json')]
    # Extraire les modèles d'appareil photo
    for json_file in json_files:
        with open(os.path.join(datas_directory, json_file), 'r') as file:
            image_data = json.load(file)
            camera_model = image_data.get("exif", {}).get("Image Model")
            if camera_model:
                camera_models.append(camera_model)
    return camera_models

# Extract camera models from the datas directory
camera_models = extract_camera_models_from_json(datas_directory)

# Count the frequency of each camera model
model_counts = Counter(camera_models)

# Print the model counts
print("Frequency of camera models:")
for model, count in model_counts.items():
    print(f"{model}: {count}")


# Initialize a list to store all dominant colors
all_dominant_colors = []

# Iterate over all JSON files in the datas directory
for json_file in os.listdir(datas_directory):
    if json_file.endswith('.json'):
        # Load JSON data
        with open(os.path.join(datas_directory, json_file), 'r') as file:
            image_data = json.load(file)
        
        # Extract dominant colors from the JSON data
        dominant_colors = image_data.get("metadata", {}).get("dominant_colors", [])
        # Convert hexadecimal colors to RGB format
        dominant_colors_rgb = [tuple(int(color[i:i+2], 16) for i in (1, 3, 5)) for color in dominant_colors if len(color) == 7]
        
        # Append to the list of all dominant colors
        all_dominant_colors.extend(dominant_colors_rgb)

# Plot the histogram of dominant colors with more bars
plt.figure(figsize=(10, 6))
plt.hist(all_dominant_colors, bins=100, color=[('#%02x%02x%02x' % color) for color in all_dominant_colors], alpha=0.75, histtype='bar')
plt.xlabel('Dominant Color (RGB)')
plt.ylabel('Frequency')
plt.title('Histogram of Dominant Colors')
plt.xticks(rotation=45)
plt.show()
