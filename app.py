import os

import psycopg2
from flask import Flask
from flask import request
from flask import jsonify
from flask_cors import CORS
from genderize import Genderize

app = Flask(__name__)
CORS(app)


@app.route('/')
def index():
    return 'Server Works!'


@app.route('/setup')
def setup():
    conn = getConnection()
    cur = conn.cursor()
    cur.execute(
        "create table if not exists authors (id serial primary key, name varchar(100), gender varchar(100), "
        "gender_source varchar(100))")
    return {"set": "up"}

@app.route('/authors')
def get_all_authors():
    conn = getConnection()
    cur = conn.cursor()
    cur.execute("select * from authors")
    data = cur.fetchall()
    conn.close()
    return jsonify(data)


@app.route('/author/gender-bulk', methods=['POST'])
def get_author_genders():
    app.logger.info(request.json)
    authors = tuple(request.json)

    # Fetch authors from DB
    query = "select authors.name, authors.gender, authors.gender_source from authors where authors.name IN %s"
    conn = getConnection()
    cur = conn.cursor()
    cur.execute(query, (authors,))
    rows = cur.fetchall()
    genders = {row[0]: (row[1], row[2]) for row in rows}

    # Find missing authors
    toFetch = list(set([x for x in authors if x not in genders]))
    fetched = []
    repsonse = Genderize(api_key="a619730661a7ce6b4f8e8e6b047046a2").get([x.split(" ")[0] for x in toFetch])
    for author, resp in zip(toFetch, repsonse):
        fetched.append((author, resp["gender"], "genderize"))

    # Add fetched to response
    for author, gender, source in fetched:
        genders[author] = (gender, source)

    # Add fetched to DB
    from psycopg2.extras import execute_values
    execute_values(cur,
                   "INSERT INTO authors (name, gender, gender_source) VALUES %s",
                   fetched)
    conn.close()
    return genders


@app.route('/author/gender')
def get_author_gender():
    name = request.args.get('name')
    conn = getConnection()
    cur = conn.cursor()
    query = "select authors.gender, authors.gender_source from authors where authors.name = %s"
    cur.execute(query, (name,))
    rows = cur.fetchall()
    if not rows:
        gender = Genderize().get([name.split(" ")[0]])[0]["gender"]
        source = "genderize"
        insert = "insert into authors (name, gender, gender_source) values (%s, %s, %s)"
        cur.execute(insert, (name, gender, source))
    else:
        gender = rows[0][0]
        source = rows[0][1]
    print(query, rows)
    conn.close()
    return {
        "name": name,
        "gender": gender,
        "source": source
    }


def getConnection():
    ON_HEROKU = os.environ.get('ON_HEROKU')

    if ON_HEROKU:
        DATABASE_URL = os.environ['HEROKU_POSTGRESQL_AQUA_URL']
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    else:
        conn = psycopg2.connect(database="book_stats", user="postgres", password="q", host="127.0.0.1", port="5432")
    conn.set_session(autocommit=True)
    return conn


if __name__ == '__main__':
    # Threaded option to enable multiple instances for multiple user access support
    port = int(os.environ.get('PORT', 5000))
    app.run(threaded=True, port=5000)
