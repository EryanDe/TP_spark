from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, row_number
from pyspark.sql.window import Window

# Initialiser une session Spark
spark = SparkSession.builder.appName("ArbresRemarquables").getOrCreate()

# Charger les données CSV dans un DataFrame Spark
df = spark.read.option("delimiter", ";").csv("file:///root/arbresremarquablesparis.csv", header=False)

# Renommer les colonnes pour une meilleure lisibilité
columns = [
    "GPS", "col2", "col3", "Arrondissement", "Famille", "Annee_Plantation",
    "Adresse", "Circonference", "Hauteur", "col10", "col11", "Genre", "Espece", "col14",
    "col15", "col16", "col17", "col18", "col19", "col20", "col21", "col22", "col23",
    "col24", "col25", "col26", "col27", "col28", "col29", "col30", "col31", "col32",
    "col33", "col34", "col35", "col36"
]
df = df.toDF(*columns)

# a. Afficher les coordonnées GPS, la taille (hauteur) et l'adresse de l'arbre le plus grand
df_filtered = df.filter(df.Hauteur != '').withColumn("Hauteur", col("Hauteur").cast("float"))
arbre_plus_grand = df_filtered.orderBy(desc("Hauteur")).first()
print(f"Coordonnées GPS : {arbre_plus_grand['GPS']}")
print(f"Hauteur : {arbre_plus_grand['Hauteur']} m")
print(f"Adresse : {arbre_plus_grand['Adresse']}")

# b. Afficher les coordonnées GPS, la taille (hauteur) et la circonférence des arbres les plus grands pour chaque arrondissement
df_filtered = df.filter(df.Circonference != '').withColumn("Circonference", col("Circonference").cast("float"))
window = Window.partitionBy("Arrondissement").orderBy(desc("Circonference"))
df_with_rank = df_filtered.withColumn("rank", row_number().over(window))
df_top_circonference = df_with_rank.filter(col("rank") == 1).select("Adresse", "GPS","Arrondissement", "Hauteur", "Circonference")
df_top_circonference.show(truncate=False)

# c. Afficher toutes les espèces d'arbre, triées par genre
df_especes = df.select("Genre", "Espece").distinct().orderBy("Genre", "Espece")
df_especes.show()

# Arrêter la session Spark
spark.stop()