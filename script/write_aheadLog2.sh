#!/bin/bash
export PGPASSWORD=postgres
psql -U postgres -h localhost -c "ALTER SYSTEM SET shared_buffers TO '4 GB';"
psql -U postgres -h localhost -c "ALTER SYSTEM SET work_mem TO '1024 MB';"

psql -U postgres -h localhost -c "ALTER SYSTEM SET checkpoint_timeout TO '10min';"
psql -U postgres -h localhost -c "ALTER SYSTEM SET max_wal_size TO '8GB';"
psql -U postgres -h localhost -c "ALTER SYSTEM SET min_wal_size TO '2GB';"
psql -U postgres -h localhost -c "SELECT pg_reload_conf();"
sudo systemctl restart postgresql

echo "Running wal buffers = -1"
psql -U postgres -h localhost -c "ALTER SYSTEM SET wal_buffers TO '-1';"
psql -U postgres -h localhost -c "SELECT pg_reload_conf();"
sudo systemctl restart postgresql

java -jar target/transactional-1.0-SNAPSHOT.jar -d jdbc:postgresql://localhost:5432/imdb -U postgres -P postgres -W 30 -R 500 -c 16

wal_buffers=(4 32 64 128 256)
for w in "${wal_buffers[@]}" ; do
    echo "--------------------Running $w wal_buffers --------------------------"
    export PGPASSWORD=postgres
    psql -U postgres -h localhost -c "ALTER SYSTEM SET wal_buffers TO '$w MB';"
    psql -U postgres -h localhost -c "SELECT pg_reload_conf();"
    sudo systemctl restart postgresql

    java -jar target/transactional-1.0-SNAPSHOT.jar -d jdbc:postgresql://localhost:5432/imdb -U postgres -P postgres -W 30 -R 500 -c 16
done

psql -U postgres -h localhost -c "ALTER SYSTEM SET wal_buffers TO '-1';"
psql -U postgres -h localhost -c "SELECT pg_reload_conf();"
sudo systemctl restart postgresql

wal_writer_delay=(10 50 100 300 500)
for w in "${wal_writer_delay[@]}" ; do
    echo "--------------------Running $w wal_writer_delay --------------------------"
    export PGPASSWORD=postgres
    psql -U postgres -h localhost -c "ALTER SYSTEM SET wal_writer_delay TO '$w ms';"
    psql -U postgres -h localhost -c "SELECT pg_reload_conf();"
    sudo systemctl restart postgresql

    java -jar target/transactional-1.0-SNAPSHOT.jar -d jdbc:postgresql://localhost:5432/imdb -U postgres -P postgres -W 30 -R 500 -c 16

done