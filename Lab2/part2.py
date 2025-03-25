from flask import Flask, request, jsonify
import psycopg2
from apscheduler.schedulers.background import BackgroundScheduler
import datetime
import os

app = Flask(__name__)

def get_db_connection():
    conn = psycopg2.connect(
        dbname="mllab2",
        user="user2",
        password="passwd2",
        host="localhost",
        port="5432"
    )
    return conn

def aggregate_data():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute('''
        SELECT country, COUNT(*) as user_count
        FROM users
        WHERE created_at >= NOW() - INTERVAL '1 minute'
        GROUP BY country
    ''')
    aggregated_data = cur.fetchall()

    cur.close()
    conn.close()

    if aggregated_data:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"aggregated_data/aggregated_data_{timestamp}.txt"
        with open(filename, 'w') as file:
            for country, user_count in aggregated_data:
                file.write(f"{country}: {user_count} users\n")
        print(f"Data aggregated and saved to {filename}")
    else:
        print("No new data to aggregate.")

scheduler = BackgroundScheduler()
scheduler.add_job(func=aggregate_data, trigger="interval", minutes=1)
scheduler.start()

@app.route('/read', methods=['GET'])
def read():
    user_id = request.args.get('id')
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM users WHERE id = %s', (user_id,))
    user = cur.fetchone()
    cur.close()
    conn.close()
    if user:
        return jsonify(user)
    else:
        return jsonify({"error": "User not found"}), 404

@app.route('/write', methods=['POST'])
def write():
    data = request.json
    name = data.get('name')
    email = data.get('email')
    country = data.get('country')

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        'INSERT INTO users (name, email, country) VALUES (%s, %s, %s)',
        (name, email, country)
    )
    conn.commit()
    cur.close()
    conn.close()
    return jsonify({"message": "User added successfully"}), 201

if __name__ == '__main__':
    app.run(debug=True)
