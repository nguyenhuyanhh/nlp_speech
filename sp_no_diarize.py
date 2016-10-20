import base64
import os

from googleapiclient.discovery import build

API_KEY = 'AIzaSyC22qOuouVqsraoV6KzCHNAzdf3gWisOwc'

service = build('speech', 'v1beta1', developerKey=API_KEY)
speech = service.speech()

request_body = {
    "audio": {
        "content": ""
    },
    "config": {
        "languageCode": "en-US",
        "encoding": "LINEAR16",
        "sampleRate": 16000
    },
}
request = speech.asyncrecognize(body=request_body)
response = request.execute()
