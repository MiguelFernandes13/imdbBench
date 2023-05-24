#!/bin/bash
export PGPASSWORD=postgres
psql -U postgres -h localhost -c "ALTER SYSTEM SET shared_buffers TO '4 GB';"
psql -U postgres -h localhost -c "ALTER SYSTEM SET work_mem TO '2048 MB';"
psql -U postgres -h localhost -c "SELECT pg_reload_conf();"

psql -U postgres -h localhost -c "ALTER SYSTEM SET checkpoint_timeout TO '10min';"
psql -U postgres -h localhost -c "ALTER SYSTEM SET max_wal_size TO '4GB';"
psql -U postgres -h localhost -c "ALTER SYSTEM SET min_wal_size TO '1GB';"
psql -U postgres -h localhost -c "SELECT pg_reload_conf();"

echo "Running with max_wal_size = 4GB, min_wal_size = 1GB"
java -jar target/transactional-1.0-SNAPSHOT.jar -d jdbc:postgresql://localhost:5432/imdb -U postgres -P postgres -W 30 -R 500 -c 16

sudo systemctl restart postgresql
echo "Running with max_wal_size = 8GB, min_wal_size = 2GB"
psql -U postgres -h localhost -c "ALTER SYSTEM SET max_wal_size TO '8GB';"
psql -U postgres -h localhost -c "ALTER SYSTEM SET min_wal_size TO '2GB';"
psql -U postgres -h localhost -c "SELECT pg_reload_conf();"

java -jar target/transactional-1.0-SNAPSHOT.jar -d jdbc:postgresql://localhost:5432/imdb -U postgres -P postgres -W 30 -R 500 -c 16

sudo systemctl restart postgresql
echo "Running with max_wal_size = 16GB, min_wal_size = 4GB"
psql -U postgres -h localhost -c "ALTER SYSTEM SET max_wal_size TO '16GB';"
psql -U postgres -h localhost -c "ALTER SYSTEM SET min_wal_size TO '4GB';"
psql -U postgres -h localhost -c "SELECT pg_reload_conf();"

java -jar target/transactional-1.0-SNAPSHOT.jar -d jdbc:postgresql://localhost:5432/imdb -U postgres -P postgres -W 30 -R 500 -c 16

