from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, explode, desc, split, lower

# Initialiser une session Spark avec un répertoire temporaire différent
spark = SparkSession.builder \
    .appName("MovieLensAnalysis") \
    .config("spark.local.dir", "C:/Temp/spark-temp") \
    .getOrCreate()

# Chemins des fichiers CSV
movies_path = "C:/Users/hamza benzy/Desktop/wordCountProject/ml-latest-small/ml-latest-small/movies.csv"
ratings_path = "C:/Users/hamza benzy/Desktop/wordCountProject/ml-latest-small/ml-latest-small/ratings.csv"
tags_path = "C:/Users/hamza benzy/Desktop/wordCountProject/ml-latest-small/ml-latest-small/tags.csv"

# Charger les fichiers CSV dans des DataFrames
movies_df = spark.read.csv(movies_path, header=True, inferSchema=True)
ratings_df = spark.read.csv(ratings_path, header=True, inferSchema=True)
tags_df = spark.read.csv(tags_path, header=True, inferSchema=True)

# Séparer les genres des films en lignes distinctes
movies_with_genres = movies_df.withColumn("genre", explode(split(col("genres"), "\\|")))

# Calculer la moyenne des notes et le nombre de notes pour chaque film
movie_ratings = ratings_df.groupBy("movieId").agg(
    avg("rating").alias("avg_rating"),
    count("rating").alias("num_ratings")
)

# Filtrer les films avec au moins 50 évaluations
filtered_movies = movie_ratings.filter(col("num_ratings") >= 50)

# Filtrer les tags pour ne garder que ceux contenant le mot "funny"
filtered_tags = tags_df.filter(lower(col("tag")).like("%funny%"))

# Joindre les DataFrames des films et des tags pour ne garder que les films tagués
movies_with_tags = filtered_movies.join(filtered_tags, "movieId").join(movies_with_genres, "movieId")

# Extraire les trois genres les plus fréquents parmi les films les mieux notés
top_genres = movies_with_tags.groupBy("genre").count().orderBy(desc("count")).limit(3)

# Trier les films par moyenne des notes (descendant) et extraire les 10 meilleurs
top_movies = movies_with_tags.orderBy(desc("avg_rating")).limit(10)

# Afficher les résultats
print("Top 10 des films tagués 'funny' les mieux notés (avec au moins 50 évaluations):")
top_movies.select("title", "avg_rating", "num_ratings", "tag").show(truncate=False)

print("Les trois genres les plus fréquents parmi ces films sont:")
top_genres.show(truncate=False)

# Arrêter la session Spark
spark.stop()