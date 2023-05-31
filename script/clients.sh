#!/bin/bash

clients=(1 2 4 8 12 16 20)
for c in "${clients[@]}" ; do
    echo "Running $c clients"
    java -jar target/transactional-1.0-SNAPSHOT.jar -d jdbc:postgresql://localhost:5432/imdb -U postgres -P postgres -W 30 -R 300 -c $c
done