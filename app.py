from flask import Flask
import requests

app = Flask(__name__)

@app.route("/")
def test():
    results = {}

    urls = [
        "https://api.binance.com/api/v3/ping",
        "https://api1.binance.com/api/v3/ping",
        "https://api2.binance.com/api/v3/ping",
        "https://api3.binance.com/api/v3/ping",
        "https://api4.binance.com/api/v3/ping",
    ]

    for url in urls:
        try:
            r = requests.get(url, timeout=10)
            results[url] = {
                "status": r.status_code,
                "response": r.text[:200]
            }
        except Exception as e:
            results[url] = {"error": str(e)}

    return results
