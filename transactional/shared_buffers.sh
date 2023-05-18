#!/bin/bash

shared_buffers=(1 2 3 4 5 6)
for s in "${shared_buffers[@]}" ; do
    echo "--------------------Running $s shared_buffers --------------------------"
    sudo psql -U postgres -c "ALTER SYSTEM SET shared_buffers TO '$sGB' \
                      -c "SELECT pg_reload_conf();"
    sudo systemctl restart postgresql
    java -jar target/transactional-1.0-SNAPSHOT.jar -d jdbc:postgresql://localhost:5432/imdb -U postgres -P postgres -W 30 -R 500 -c 16

    start=$(date +%s.%N)
    psql -U postgres -d imdb -a -f ../analytical/1.sql
    end=$(date +%s.%N)
    runtime=$(echo "1.sql: $end - $start" | bc -l)

    start=$(date +%s.%N)
    psql -U postgres -d imdb -a -f ../analytical/2.sql
    end=$(date +%s.%N)
    runtime=$(echo "2.sql: $end - $start" | bc -l)

    start=$(date +%s.%N)
    psql -U postgres -d imdb -a -f ../analytical/3.sql
    end=$(date +%s.%N)
    runtime=$(echo "3.sql: $end - $start" | bc -l)
done
