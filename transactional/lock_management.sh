#!/bin/bash
export PGPASSWORD=postgres
psql -U postgres -h localhost -c "ALTER SYSTEM SET shared_buffers TO '4 GB';"
psql -U postgres -h localhost -c "ALTER SYSTEM SET work_mem TO '1024 MB';"

psql -U postgres -h localhost -c "SELECT pg_reload_conf();"

deadlock_timeout=(1 5 10 30 60)
for d in "${deadlock_timeout[@]}" ; do
    echo "--------------------Running $d deadlock_timeout --------------------------"
    export PGPASSWORD=postgres
    psql -U postgres -h localhost -c "ALTER SYSTEM SET deadlock_timeout TO '$d s';"
    psql -U postgres -h localhost -c "SELECT pg_reload_conf();"
    sudo systemctl restart postgresql

    java -jar target/transactional-1.0-SNAPSHOT.jar -d jdbc:postgresql://localhost:5432/imdb -U postgres -P postgres -W 30 -R 500 -c 16

done