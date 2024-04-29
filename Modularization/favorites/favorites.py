import ipywidgets as widgets
from os import listdir
from IPython.display import display

# Récupérer les chemins des images
image_paths = ["./images/" + file for file in listdir("./images")]

# Créer des cases à cocher pour chaque image
checkboxes = [widgets.Checkbox(value=False, description='Favorite') for _ in range(len(image_paths))]

# Créer des widgets d'image pour chaque image
image_widgets = [widgets.Image(value=open(img, "rb").read(), format='png', width=100, height=100) for img in image_paths]

# Affichage des images et des cases à cocher
for image_widget, checkbox in zip(image_widgets, checkboxes):
    display(image_widget, checkbox)

# Bouton pour obtenir les images sélectionnées
button = widgets.Button(description="Select")

# Fonction pour obtenir les images sélectionnées
def get_selected_images(btn):
    selected_paths = [image_paths[i] for i, checkbox in enumerate(checkboxes) if checkbox.value]
    print("Selected Images:")
    for path in selected_paths:
        print(path)

# Lier l'événement de clic sur le bouton à la fonction
button.on_click(get_selected_images)

# Affichage du bouton
display(button)
