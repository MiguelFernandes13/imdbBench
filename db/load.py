import argparse
import os
import psycopg2
import psycopg2.extras
import csv
from multiprocessing import Pool, cpu_count
import gzip
import requests
import shutil
import json
import subprocess
import hashlib
import base64
import random
from datetime import datetime, timedelta

# url with the dataset
URL_PREFIX = 'https://datasets.imdbws.com/'
# directory to store the data files
DATA_DIR = 'data'
# list of data files
FILES = ['name.basics.tsv', 'title.akas.tsv', 'title.basics.tsv', 'title.crew.tsv',
         'title.episode.tsv', 'title.principals.tsv']
# number of entries to load to the database each time
BATCH_SIZE = int(1e6)
# list of country codes
COUNTRY_CODES = ['AF','AX','AL','DZ','AS','AD','AO','AI','AQ','AG','AR','AM','AW','AU','AT','AZ','BS','BH','BD','BB','BY','BE','BZ','BJ','BM','BT','BO','BA','BW','BV','BR','IO','BN','BG','BF','BI','KH','CM','CA','CV','KY','CF','TD','CL','CN','CX','CC','CO','KM','CG','CD','CK','CR','CI','HR','CU','CY','CZ','DK','DJ','DM','DO','EC','EG','SV','GQ','ER','EE','ET','FK','FO','FJ','FI','FR','GF','PF','TF','GA','GM','GE','DE','GH','GI','GR','GL','GD','GP','GU','GT','GG','GN','GW','GY','HT','HM','VA','HN','HK','HU','IS','IN','ID','IR','IQ','IE','IM','IL','IT','JM','JP','JE','JO','KZ','KE','KI','KP','KR','KW','KG','LA','LV','LB','LS','LR','LY','LI','LT','LU','MO','MK','MG','MW','MY','MV','ML','MT','MH','MQ','MR','MU','YT','MX','FM','MD','MC','MN','MS','MA','MZ','MM','NA','NR','NP','NL','AN','NC','NZ','NI','NE','NG','NU','NF','MP','NO','OM','PK','PW','PS','PA','PG','PY','PE','PH','PN','PL','PT','PR','QA','RE','RO','RU','RW','SH','KN','LC','PM','VC','WS','SM','ST','SA','SN','CS','SC','SL','SG','SK','SI','SB','SO','ZA','GS','ES','LK','SD','SR','SJ','SZ','SE','CH','SY','TW','TJ','TZ','TH','TL','TG','TK','TO','TT','TN','TR','TM','TC','TV','UG','UA','AE','GB','US','UM','UY','UZ','VU','VE','VN','VG','VI','WF','EH','YE','ZM']
# start date of our "app"
START_DATE = datetime.now() - timedelta(days=3650)

# downloads the content at url and saves it to dest
def download(url, dest):
    print(f'Getting {url} -> {dest}')
    r = requests.get(url)
    with open(dest, 'wb') as f:
        f.write(r.content)


# extracts a giz file
def extract(src, dest):
    print(f'Extracting {src} -> {dest}')
    with gzip.open(src, 'rb') as fSrc:
        with open(dest, 'wb') as fDest:
            shutil.copyfileobj(fSrc, fDest)


# downloads the data files (if not already present at DATA_DIR)
# and extracts them (if not already extracted at DATA_DIR)
def prepareDataset():
    p = Pool(len(FILES))
    jobs = []

    try:
        os.mkdir(os.path.join(os.path.dirname(__file__), DATA_DIR))
    except:
        pass

    # gz
    for file in FILES:
        fullName = os.path.join(os.path.dirname(__file__), DATA_DIR, file)
        if not os.path.exists(fullName + '.gz') and not os.path.exists(fullName):
            job = p.apply_async(download, (URL_PREFIX + file + '.gz', fullName + '.gz'))
            jobs.append(job)
    [x.get() for x in jobs]
    jobs.clear()

    # tsv
    for file in FILES:
        fullName = os.path.join(os.path.dirname(__file__), DATA_DIR, file)
        if not os.path.exists(fullName):
            job = p.apply_async(extract, (fullName + '.gz', fullName))
            jobs.append(job)
    [x.get() for x in jobs]


# connects to a PostgreSQL database
def connectDatabase(connStr):
    return psycopg2.connect(connStr)


# converts \N to None
def convertRowNullFields(row):
    return {k:(v if v != r'\N' else None) for k, v in row.items()}


# loads rows to the respective table in the database
def loadRows(rows, tablename, connStr):
    filename = os.path.join(os.path.dirname(__file__), DATA_DIR, f'{tablename}.csv')

    with open(filename, 'w', newline='', encoding='utf8') as f:
        writer = csv.writer(f, delimiter=',', quoting=csv.QUOTE_NONE, escapechar='\\')
        writer.writerows(rows)

    env = os.environ.copy()
    env['PGCLIENTENCODING'] = 'utf-8'
    process = subprocess.Popen(['psql', '-c',
                                rf"\copy {tablename} from {filename} delimiter ',' null as ''", connStr],
                               env=env, stdout=subprocess.DEVNULL)
    process.wait()

    os.remove(filename)


def processTitleBasics(reader, connStr):
    titleRows = []
    titleGenreRows = []
    genres = {}
    currentGenreIndex = 1

    for row in reader:
        row = convertRowNullFields(row)
        titleRows.append((row['tconst'], row['titleType'], row['primaryTitle'], row['originalTitle'],
                     row['isAdult'], row['startYear'], row['endYear'], row['runtimeMinutes']))
        if row['genres'] is not None:
            for genre in row['genres'].split(','):
                if genre not in genres:
                    genres[genre] = currentGenreIndex
                    currentGenreIndex += 1
                titleGenreRows.append((row['tconst'], genres[genre]))

        if len(titleRows) >= BATCH_SIZE:
            loadRows(titleRows, 'title', connStr)
            titleRows = []
            loadRows(titleGenreRows, 'titleGenre', connStr)
            titleGenreRows = []

    loadRows(titleRows, 'title', connStr)
    loadRows(titleGenreRows, 'titleGenre', connStr)
    loadRows([(v, k) for k, v in genres.items()], 'genre', connStr)


def processTitleAkas(reader, connStr):
    titleAkasRows = []

    for row in reader:
        row = convertRowNullFields(row)
        titleAkasRows.append((row['titleId'], row['ordering'], row['title'], row['region'], row['language']))

        if len(titleAkasRows) >= BATCH_SIZE:
            loadRows(titleAkasRows, 'titleAkas', connStr)
            titleAkasRows = []

    loadRows(titleAkasRows, 'titleAkas', connStr)


def processNameBasics(reader, connStr):
    nameRows = []
    nameProfessionRows = set()
    professions = {}
    currentProfessionId = 1
    nameTitles = []

    for row in reader:
        row = convertRowNullFields(row)
        nameRows.append((row['nconst'], row['primaryName'], row['birthYear'], row['deathYear']))
        for profession in row['primaryProfession'].split(','):
            if profession not in professions:
                professions[profession] = currentProfessionId
                currentProfessionId += 1
            nameProfessionRows.add((row['nconst'], professions[profession]))
        if row['knownForTitles'] is not None:
            for title in row['knownForTitles'].split(','):
                nameTitles.append((row['nconst'], title))

        if len(nameRows) >= BATCH_SIZE:
            loadRows(nameRows, 'name', connStr)
            nameRows = []
            loadRows(nameProfessionRows, 'namePrimaryProfession', connStr)
            nameProfessionRows = set()
            loadRows(nameTitles, 'nameKnownForTitles', connStr)
            nameTitles = []

    loadRows(nameRows, 'name', connStr)
    loadRows(nameProfessionRows, 'namePrimaryProfession', connStr)
    loadRows(nameTitles, 'nameKnownForTitles', connStr)
    loadRows([(v, k) for k, v in professions.items()], 'profession', connStr)


def processTitlePrincipals(reader, connStr):
    titlePrincipalsRows = []
    categories = {}
    currentCategoryId = 1
    jobs = {}
    currentJobId = 1
    titleCharacters = set()

    for row in reader:
        row = convertRowNullFields(row)
        if row['category'] not in categories:
            categories[row['category']] = currentCategoryId
            currentCategoryId += 1
        if row['job'] not in jobs:
            jobs[row['job']] = currentJobId
            currentJobId += 1
        titlePrincipalsRows.append((row['tconst'], row['ordering'], row['nconst'], categories[row['category']],
                                    jobs[row['job']]))
        if row['characters'] is not None:
            for character in json.loads(row['characters']):
                titleCharacters.add((row['tconst'], row['nconst'], character))

        if len(titlePrincipalsRows) >= BATCH_SIZE:
            loadRows(titlePrincipalsRows, 'titlePrincipals', connStr)
            titlePrincipalsRows = []
            loadRows(titleCharacters, 'titlePrincipalsCharacters', connStr)
            titleCharacters = set()

    loadRows(titlePrincipalsRows, 'titlePrincipals', connStr)
    loadRows(titleCharacters, 'titlePrincipalsCharacters', connStr)
    loadRows([(v, k) for k, v in categories.items()], 'category', connStr)
    loadRows([(v, k) for k, v in jobs.items()], 'job', connStr)


def processTitleCrew(reader, connStr):
    titleCrewRows = []

    for row in reader:
        row = convertRowNullFields(row)
        if row['directors'] is not None:
            for director in row['directors'].split(','):
                titleCrewRows.append((row['tconst'], director, 'director'))
        if row['writers'] is not None:
            for writer in row['writers'].split(','):
                titleCrewRows.append((row['tconst'], writer, 'writer'))

        if len(titleCrewRows) >= BATCH_SIZE:
            loadRows(titleCrewRows, 'titleCrew', connStr)
            titleCrewRows = []

    loadRows(titleCrewRows, 'titleCrew', connStr)


def processTitleEpisode(reader, connStr):
    titleEpisodeRows = []

    for row in reader:
        row = convertRowNullFields(row)
        titleEpisodeRows.append((row['tconst'], row['parentTconst'], row['seasonNumber'], row['episodeNumber']))

        if len(titleEpisodeRows) >= BATCH_SIZE:
            loadRows(titleEpisodeRows, 'titleEpisode', connStr)
            titleEpisodeRows = []

    loadRows(titleEpisodeRows, 'titleEpisode', connStr)


# reads a data file and loads it to the database
def processDataFile(filename, connStr):
    print(f'Loading {filename}')

    with open(filename, encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='\t', quoting=csv.QUOTE_NONE)

        if 'title.basics' in filename:
            processTitleBasics(reader, connStr)
        elif 'title.akas' in filename:
            processTitleAkas(reader, connStr)
        elif 'name.basics' in filename:
            processNameBasics(reader, connStr)
        elif 'title.principals' in filename:
            processTitlePrincipals(reader, connStr)
        elif 'title.crew' in filename:
            processTitleCrew(reader, connStr)
        elif 'title.episode' in filename:
            processTitleEpisode(reader, connStr)

    print(f'Loading {filename} done')


def populateUsers(connStr, nUsers=100000):
    print('Populating users')
    users = []

    for i in range(nUsers):
        users.append((i + 1, f"user{i+1}@email.com",
                      base64.b64encode(hashlib.sha512(bytes(f'user-{i + 1}', 'ascii')).digest()).decode('ascii'),
                      f'User {i}', random.choice(COUNTRY_CODES)))

    loadRows(users, 'users', connStr)
    print('Populate users done')


# generates a random datetime between START_DATE and now(), >= minYear
def randomDate(minYear):
    if minYear is not None:
        start = max(datetime(minYear, 1, 1), START_DATE)
    else:
        start = START_DATE
    daysRange = max((datetime.now() - start).days, 1)

    return start + timedelta(days=random.randint(0, daysRange - 1),
                             hours=random.randint(0, 24),
                             minutes=random.randint(0, 60))


def populateUserList(connStr, nUsers=100000, nTitlesPerUser=100, pNotFinished=0.1, pNotStarted=0.4):
    print("Populating users's lists")
    conn = connectDatabase(connStr)
    cursor = conn.cursor()
    cursor.execute("select id, start_year, coalesce(runtime_minutes, 60) from title where title_type = 'movie' or title_type = 'tvEpisode'")
    titles = cursor.fetchall()
    conn.close()
    userList = []
    userHistory = []

    for i in range(nUsers):
        userTitles = set()
        for _ in range(nTitlesPerUser):
            userTitles.add(random.choice(titles))
        for title in userTitles:
            addedDate = randomDate(title[1])
            r = random.random()
            if r <= pNotStarted:
                userList.append((i + 1, title[0], addedDate))
            elif r <= pNotStarted + pNotFinished:
                userList.append((i + 1, title[0], addedDate))
                userHistory.append((i + 1, title[0], random.randint(0, title[2]), addedDate,
                                    addedDate + timedelta(days=random.randint(0, 7)), None))
            else:
                userHistory.append((i + 1, title[0], title[2], addedDate,
                    addedDate + timedelta(days=random.randint(0, 2)), random.randint(1, 5)))

    loadRows(userList, 'userList', connStr)
    loadRows(userHistory, 'userHistory', connStr)
    print("Populate users's lists done")


def populateUserGenres(connStr, nUsers=100000, minGenres=1, maxGenres=3):
    print("Populating users's genres")
    conn = connectDatabase(connStr)
    cursor = conn.cursor()
    cursor.execute("select id from genre")
    genres = cursor.fetchall()
    conn.close()
    userGenres = []

    for i in range(nUsers):
        nGenres = random.randint(minGenres, maxGenres)
        selectedGenres = set()

        while len(selectedGenres) < nGenres:
            selectedGenres.add(random.choice(genres)[0])

        userGenres.extend([(i + 1, genre) for genre in selectedGenres])

    loadRows(userGenres, 'userGenre', connStr)
    print("Populate users's genres done")


# executes the schema.sql script in the provided database
def populateDatabase(connStr):
    conn = connectDatabase(connStr)
    cursor = conn.cursor()
    with open(os.path.join(os.path.dirname(__file__), 'schema.sql')) as f:
        cursor.execute(f.read())
    conn.commit()
    conn.close()

    pool = Pool(cpu_count())
    jobs = []
    for file in FILES:
        fullname = os.path.join(os.path.dirname(__file__), DATA_DIR, file)
        jobs.append(pool.apply_async(processDataFile, (fullname, connStr)))
    jobs.append(pool.apply_async(populateUsers, (connStr,)))
    [x.get() for x in jobs]
    jobs.clear()

    jobs.append(pool.apply_async(populateUserList, (connStr,)))
    jobs.append(pool.apply_async(populateUserGenres, (connStr,)))
    [x.get() for x in jobs]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-H', '--host', help='PostgreSQL host', required=True)
    parser.add_argument('-P', '--port', help='PostgreSQL port', default=5432)
    parser.add_argument('-d', '--database', help='Database name', required=True)
    parser.add_argument('-u', '--user', help='Username', required=True)
    parser.add_argument('-p', '--password', help='Password', required=True)
    args = parser.parse_args()

    prepareDataset()
    connStr = f"postgresql://{args.user}:{args.password}@{args.host}:{args.port}/{args.database}"
    populateDatabase(connStr)


if __name__ == '__main__':
    main()
