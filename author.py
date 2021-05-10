import psycopg2
import requests
from genderize import Genderize
from flask import Flask
from flask import request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)


@app.route('/')
def index():
    return 'Server Works!'


@app.route('/author/gender-bulk', methods=['POST'])
def get_author_genders():
    app.logger.info(request.json)
    authors = tuple(request.json)

    # Fetch authors from DB
    query = "select authors.name, authors.gender, authors.gender_source from authors where authors.name IN %s"
    conn = psycopg2.connect(database="book_stats", user="postgres", password="q", host="127.0.0.1", port="5432")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    cur.execute(query, (authors,))
    rows = cur.fetchall()
    genders = {row[0]: (row[1], row[2]) for row in rows}

    # Find missing authors
    toFetch = list(set([x for x in authors if x not in genders]))
    fetched = []
    repsonse = Genderize().get([x.split(" ")[0] for x in toFetch])
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
    return genders


@app.route('/author/gender')
def get_author_gender():
    name = request.args.get('name')
    conn = psycopg2.connect(database="book_stats", user="postgres", password="q", host="127.0.0.1", port="5432")
    conn.set_session(autocommit=True)
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


if __name__ == '__main__':
    # Threaded option to enable multiple instances for multiple user access support
    app.run(threaded=True, port=5000)