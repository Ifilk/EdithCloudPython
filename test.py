import requests
import json

response = requests.post('http://localhost:8080/stream', json.dumps({'id': 'test'}),
                         headers={'Content-Type': 'application/json'}, stream=True)

for chunk in response.iter_content(1024, True):
    print(chunk)
