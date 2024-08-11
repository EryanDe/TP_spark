#!/usr/bin/env python3
import sys
import csv

# Lecture de l'entrée standard
reader = csv.reader(sys.stdin)

# Ignore l'en-tête
next(reader)

# Mapper pour émettre le nom du boss et ses HP
for row in reader:
    try:
        name = row[1]  # Nom du boss
        hp = row[4]  # Points de vie du boss
        # Émettre la clé (nom du boss) et la valeur (points de vie)
        print(f"{name}\t{hp}")
    except IndexError:
        continue









