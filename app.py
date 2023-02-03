from flask import Flask
import redis

app = Flask(__name__)
cache = redis.StrictRedis(host='redis', port=6379, db=0)
print("REDIS ping:", cache.ping())

@app.route('/')
def hello_geek():
    print("APP hello geek")
    return f"<h1>Hello from Flask & Docker</h1><h1>redis: up_and_running={cache.ping()}</h1>"


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)
