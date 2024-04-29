import os
import json
import numpy as np
import math
import matplotlib.pyplot as plt
from PIL import Image
from sklearn.cluster import KMeans

def extract_dominant_colors(image):
    """
    Extracts the dominant colors from an image using K-means clustering.

    Parameters:
    - image: PIL.Image object representing the input image.

    Returns:
    - dominant_colors: Array of RGB values representing the dominant colors in the image.
    """
    numarray = np.array(image.getdata(), dtype=np.uint8)
    clusters = KMeans(n_clusters=4, n_init=2)
    clusters.fit(numarray)
    return clusters.cluster_centers_

# Specify the path to the directory containing images
images_directory = "images"
datas_directory = "datas"


# Iterate over each file in the images directory
for filename in os.listdir(images_directory):
    image_filename = os.path.join(images_directory, filename)
    # Check if the path is a file (not a directory)
    if os.path.isfile(image_filename):
        try:
            # Open the image
            image = Image.open(image_filename)
            # Extract dominant colors
            dominant_colors = extract_dominant_colors(image)

            hex_colors = ['#%02x%02x%02x' % tuple(color.astype(int)) for color in dominant_colors]
            
            # Load the corresponding JSON file
            json_filename = os.path.splitext(filename)[0] + ".json"
            json_filepath = os.path.join(datas_directory, json_filename)
            with open(json_filepath, 'r') as f:
                data = json.load(f)
                
            data['metadata']['dominant_colors'] = hex_colors
            with open(json_filepath, 'w') as f:
                json.dump(data, f, indent=4)
            
            # Plot dominant colors
            plt.figure(figsize=(6, 2))
            for i, color in enumerate(dominant_colors):
                plt.subplot(1, 4, i + 1)
                plt.imshow([[color / 255]])
                plt.axis('off')
            plt.suptitle(f"Dominant Colors of {filename}")
            plt.show()

        except Exception as e:
            print(f"Error processing {image_filename}: {e}")