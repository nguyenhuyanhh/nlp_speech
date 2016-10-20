import base64
import json
import os

from googleapiclient.discovery import build

# init
cur_dir = os.path.dirname(os.path.realpath(__name__))
data_dir = os.path.join(cur_dir, 'data/')
API_KEY = 'AIzaSyC22qOuouVqsraoV6KzCHNAzdf3gWisOwc'
service = build('speech', 'v1beta1', developerKey=API_KEY)
speech = service.speech()


def recognize_file(filename):
    path = os.path.join(data_dir, filename)
    with open(path, 'rb') as file:
        content = base64.b64encode(file.read())
        content_string = content.decode('utf-8')
    request_body = {
        "audio": {
            "content": content_string
        },
        "config": {
            "languageCode": "en-US",
            "encoding": "LINEAR16",
            "sampleRate": 16000
        },
    }
    request = speech.syncrecognize(body=request_body)
    response = request.execute()
    return response

response = print(recognize_file('test-2.wav'))
print(response)
