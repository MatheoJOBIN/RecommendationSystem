# Utiliser une image de base contenant Python et Spark
FROM python:3.8-slim

# Installer les dépendances nécessaires
RUN pip install matplotlib numpy pillow pyspark scikit-learn

# Copier le dossier color dans le conteneur
COPY ./color.py /color/

# Définir le répertoire de travail
WORKDIR /color/

# Exécuter le script Python à l'intérieur du conteneur
CMD ["python", "color.py"]
