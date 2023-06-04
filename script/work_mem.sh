#!/bin/bash
export PGPASSWORD=postgres
psql -U postgres -h localhost -c "ALTER SYSTEM SET shared_buffers TO '4 GB';"
psql -U postgres -h localhost -c "SELECT pg_reload_conf();"
work_mem=(4 16 64 256 1024 2048 4096 8192 12288)
for s in "${work_mem[@]}" ; do
    echo "--------------------Running $s work_mem --------------------------"
    export PGPASSWORD=postgres
    psql -U postgres -h localhost -c "ALTER SYSTEM SET work_mem TO '$s MB';"
    psql -U postgres -h localhost -c "SELECT pg_reload_conf();"
    sudo systemctl restart postgresql

    java -jar target/transactional-1.0-SNAPSHOT.jar -d jdbc:postgresql://localhost:5432/imdb -U postgres -P postgres -W 30 -R 500 -c 16

    start=$(date +%s.%N)
    psql -U postgres -h localhost -d imdb -a -f ../analytical/1.sql
    end=$(date +%s.%N)
    runtime=$(echo "$end - $start" | bc)
    echo "Runtime was $runtime"

    start=$(date +%s.%N)
    psql -U postgres -h localhost -d imdb -a -f ../analytical/2.sql
    end=$(date +%s.%N)
    runtime=$(echo "$end - $start" | bc)
    echo "2.sql: $runtime"

    start=$(date +%s.%N)
    psql -U postgres -h localhost -d imdb -a -f ../analytical/3.sql
    end=$(date +%s.%N)
    runtime=$(echo "$end - $start" | bc)
    echo "3.sql: $runtime"
done
