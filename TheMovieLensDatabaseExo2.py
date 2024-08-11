from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from prettytable import PrettyTable  # Importer la bibliothèque prettytable

# Configuration et initialisation de SparkContext
conf = SparkConf().setAppName("MovieLensAnalysis").set("spark.local.dir", "/root/spark-temp")
sc = SparkContext(conf=conf)

# Initialiser SQLContext pour utiliser des fonctions SQL si nécessaire
sqlContext = SQLContext(sc)

# Chemins des fichiers CSV
movies_path = "file:///root/ml-latest-small/ml-latest-small/movies.csv"
ratings_path = "file:///root/ml-latest-small/ml-latest-small/ratings.csv"
tags_path = "file:///root/ml-latest-small/ml-latest-small/tags.csv"

# Charger les fichiers CSV dans des RDDs
movies_rdd = sc.textFile(movies_path)
ratings_rdd = sc.textFile(ratings_path)
tags_rdd = sc.textFile(tags_path)


# Enlever l'en-tête et convertir les données en tuples
def parse_movies(line):
    parts = line.split(',')
    if len(parts) < 3:
        return None
    return (int(parts[0].strip()), parts[1].strip().strip('"'), parts[2].strip())


def parse_ratings(line):
    parts = line.split(',')
    if len(parts) < 4:
        return None
    return (int(parts[1].strip()), float(parts[2].strip()))


def parse_tags(line):
    parts = line.split(',')
    if len(parts) < 3:
        return None
    return (int(parts[1].strip()), parts[2].strip())


# Filtrer les en-têtes et transformer les lignes en tuples
header_movies = movies_rdd.first()
movies_rdd = movies_rdd.filter(lambda line: line != header_movies).map(parse_movies)

header_ratings = ratings_rdd.first()
ratings_rdd = ratings_rdd.filter(lambda line: line != header_ratings).map(parse_ratings)

header_tags = tags_rdd.first()
tags_rdd = tags_rdd.filter(lambda line: line != header_tags).map(parse_tags)


# Séparer les genres des films en lignes distinctes
def explode_genres(movie):
    if movie is None:
        return []
    movieId, title, genres = movie
    genres_list = genres.split('|')
    return [(movieId, title, genre) for genre in genres_list]


movies_with_genres_rdd = movies_rdd.flatMap(explode_genres)

# Calculer la moyenne des notes et le nombre de notes pour chaque film
ratings_rdd = ratings_rdd.map(lambda x: (x[0], (x[1], 1))) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
    .mapValues(lambda x: x[0] / x[1])  # Calcul de la moyenne

# Joindre les genres avec les notes moyennes
movies_with_ratings = movies_with_genres_rdd.map(lambda x: (x[0], (x[1], x[2]))) \
    .join(ratings_rdd) \
    .map(lambda x: (x[1][0][1], x[1][0][0], x[1][1]))  # (title, genre, avg_rating)


# Afficher les top 10 résultats dans un tableau
def print_top_10_as_table(result):
    table = PrettyTable()
    table.field_names = ["Title", "Genre", "Average Rating"]

    for item in result:
        table.add_row(item)

    print(table)


# Collecter les top 10 résultats et les imprimer sous forme de tableau
top_10_results = movies_with_ratings.take(10)
print_top_10_as_table(top_10_results)

# Arrêter le SparkContext
sc.stop()
