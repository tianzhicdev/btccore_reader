from flask import Flask, jsonify
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

def get_db_connection():
    conn = psycopg2.connect(
        dbname="bitcoin",
        user="abc",
        password="12345",
        host="localhost"
    )
    return conn

@app.route('/hodls', methods=['GET'])
def get_hodls():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT * FROM hodls;")
    hodls = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify(hodls)

if __name__ == '__main__':
    app.run(port=3001)
