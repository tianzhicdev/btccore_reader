from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
import requests
import json
import os
import logging


# Configure logging
logging.basicConfig(filename='/tmp/backend.log', level=logging.INFO, 
                    format='%(asctime)s %(levelname)s:%(message)s')

with open('/usr/local/.secrets/openai', 'r') as file:
    OPENAI_TOKEN = file.read().strip()


logging.info(f"OPENAI_TOKEN value: {OPENAI_TOKEN}")

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
    logging.info("Received request for /hodls")
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT * FROM hodls;")
    hodls = cursor.fetchall()
    cursor.close()
    conn.close()
    logging.info("Successfully fetched hodls data")
    return jsonify(hodls)

@app.route('/query', methods=['POST'])
def execute_query():
    query = request.json.get('query')
    if not query:
        logging.warning("No query provided in /query request")
        return jsonify({"error": "No query provided"}), 400

    try:
        logging.info(f"Executing query: {query}")
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        logging.info("Query executed successfully")
        return jsonify(results)
    except Exception as e:
        logging.error(f"Error executing query: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/completion', methods=['POST'])
def completion():
    data = request.json
    prompt = data.get('prompt')
    response_format = json.loads(data.get('response_format', '{}'))

    try:
        logging.info(f"Sending completion request with prompt: {prompt}")
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
        curl_command = (
            f"curl -X POST https://api.openai.com/v1/chat/completions "
            f"-H 'Content-Type: application/json' "
            f"-H 'Authorization: Bearer {OPENAI_TOKEN}' "
            f"-d '{{\"model\": \"gpt-4o-2024-08-06\", \"messages\": [{{\"role\": \"system\", \"content\": \"You are a helpful assistant.\"}}, {{\"role\": \"user\", \"content\": \"{prompt}\"}}], \"response_format\": {json.dumps(response_format)}}}'"
        )
        logging.info(f"Curl equivalent: {curl_command}")

        if response.status_code != 200:
            logging.error(f"HTTP error! status: {response.status_code}")
            return jsonify({"error": f"HTTP error! status: {response.status_code}"}), response.status_code

        completion = response.json()
        events = completion.get('choices', [{}])[0].get('message', {}).get('content', [])
        logging.info("Completion request successful")
        return jsonify(events)
    except Exception as e:
        logging.error(f"Error in completion request: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    logging.info("Starting Flask app")
    app.run(host='0.0.0.0', port=3001)
