from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
import requests
import json
import os

OPENAI_TOKEN = os.getenv('OPENAI_TOKEN')

app = Flask(__name__)

CORS(app, resources={r"/*": {"origins": "*"}})

def get_db_connection():
    conn = psycopg2.connect(
        dbname="bitcoin",
        user="abc",
        password="12345",
        host="localhost",
        port="3004"
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

@app.route('/query', methods=['POST'])
def execute_query():
    query = request.json.get('query')
    if not query:
        return jsonify({"error": "No query provided"}), 400

    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/completion', methods=['POST'])
def completion():
    data = request.json
    prompt = data.get('prompt')
    response_format = json.loads(data.get('response_format', '{}'))

    try:
        response = requests.post(
            'https://api.openai.com/v1/chat/completions',
            headers={
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {OPENAI_TOKEN}'

            },
            json={
                "model": "gpt-4o-2024-08-06",
                "messages": [
                    {"role": "system", "content": "You are a helpful assistant."},
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                "response_format": response_format
            }
        )

        if response.status_code != 200:
            return jsonify({"error": f"HTTP error! status: {response.status_code}"}), response.status_code

        completion = response.json()
        events = completion.get('choices', [{}])[0].get('message', {}).get('content', [])
        return jsonify(events)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(port=3001)
