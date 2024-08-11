# TP FINAL - Big Data 
Ce dépôt contient les solutions aux trois exercices du TP final en Big Data. Le TP est réalisé par Hamza BENALIA et Eryan DELMON 

## Exercice 1 : Il est où le bel arbre ?
Description
Dans cet exercice, vous travaillerez avec les données OpenData Paris concernant les arbres remarquables de Paris. L'objectif est d'utiliser PySpark pour effectuer différentes analyses sur ces données, notamment :

Afficher les coordonnées GPS, la taille et l’adresse de l’arbre le plus grand.
Afficher les coordonnées GPS des arbres avec les plus grandes circonférences pour chaque arrondissement de Paris.
Afficher toutes les espèces d’arbres triées par genre.
Fichiers

ScriptPySparkExo1.py : Contient les solutions PySpark pour les trois sous-tâches de l'exercice 1.
Instructions
Téléchargez le fichier CSV contenant les données des arbres depuis le site OpenData Paris.
Suivez les instructions pour prétraiter les données et les copier dans HDFS.
Exécutez les scripts fournis dans ce dépôt pour effectuer les analyses demandées par exemple:  python3 PySparkScript.py 

## Exercice 2 : The MovieLens Database
Description
Cet exercice implique l'utilisation de la base de données MovieLens, qui contient des informations sur des films, des notes et des tags attribués par les utilisateurs. L'objectif est de proposer une requête PySpark sophistiquée et originale qui interroge plusieurs fichiers de cette base.
Fichiers

TheMovieLensDatabaseExo2.py : Contient la requête PySpark demandée pour l'exercice 2. On a pas utilisé PySpark.sql 
Instructions
Téléchargez et décompressez la base de données MovieLens.
Exécutez le script fourni pour effectuer l'analyse par exemple : spark-submit --deploy-mode client --master local[2] TheMovieLensDatabase.py input/ml-latest-small/m
l-latest-small/

## Exercice 3 : Construction d'une architecture Big Data
Description
Dans cet exercice, vous devez mettre en place un traitement batch avec Hadoop et MapReduce sur un dataset de votre choix. Le résultat attendu est une capture d'écran de votre interface web Hadoop (http://localhost:8088).
Fichiers
mapperExo3.py : Script Python pour la phase de mappage.
reduceExo3.py : Script Python pour la phase de réduction.
Capture d'écran : La capture d'écran du résultat sera incluse ici.

- ![Résultat du traitement batch](https://github.com/EryanDe/TP_spark/blob/main/images/batch1.png)
- - ![Résultat du traitement batch](https://github.com/EryanDe/TP_spark/blob/main/images/batch2.png)
  - - ![Résultat du traitement batch](https://github.com/EryanDe/TP_spark/blob/main/images/batch3.png)



il faut exécuté cette commande pour démmarer le Batch : hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar     -input /user/hadoop/input/bosses.csv     -output /user
/hadoop/output     -mapper /root/mapreduce/map.py     -reducer /root/mapreduce/red.py     -file /root/mapreduce/map.py     -file /root/mapreduce/red.py


## Lien Image Docker
Lien : https://hub.docker.com/repository/docker/hamza3991/hadoop-tp-final/general
