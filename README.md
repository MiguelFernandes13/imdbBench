# IMDbBench

### Load Data

- Install requirements (with `pip`):
```shell
pip3 install -r db/requirements.txt
```

- Load:
```shell
# replace 'HOST', 'PORT', 'DBNAME', 'USER', and 'PASSWORD' with the
# respective connection variables.
python3 db/load.py -H HOST -P PORT -d DBNAME -u USER -p PASSWORD
# E.g.:
#   python3 db/load.py -H localhost -P 5432 -d imdb -u postgres -p postgres
```

### Transactional workload (java 17)

(database must be already loaded)

- Install:
```shell
cd transactional
mvn package
```

- Run:
```shell
# replace the connection, warmup, runtime, and client variables with the respective values
java -jar target/transactional-1.0-SNAPSHOT.jar -d jdbc:postgresql://HOST:PORT/DBNAME -U USER -P PASSWORD -W WARMUP -R RUNTIME -c CLIENTS
# E.g.:
java -jar target/transactional-1.0-SNAPSHOT.jar -d jdbc:postgresql://localhost:5432/imdb -U postgres -P postgres -W 30 -R 300 -c 1
```

### Analytical workload

The analytical queries can be found in the `analytical` folder.
