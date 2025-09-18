import requests

try:
    res = requests.get("http://localhost:9200", auth=("elastic", "elastic123"))
    if res.status_code == 200:
        print("Elasticsearch connected successfully!")
        print("Cluster info:", res.json())
    else:
        print("Elasticsearch connection failed:", res.text)
except Exception as e:
    print("Error connecting to Elasticsearch:", e)
