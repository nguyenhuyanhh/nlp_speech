import base64
import json
import os
import time

from googleapiclient.discovery import build
import sox

# init
cur_dir = os.path.dirname(os.path.realpath(__name__))
data_dir = os.path.join(cur_dir, 'data/')
API_KEY = 'AIzaSyC22qOuouVqsraoV6KzCHNAzdf3gWisOwc'
service = build('speech', 'v1beta1', developerKey=API_KEY)
speech = service.speech()


def convert(file):
    filename = os.path.splitext(file)[0]
    path_in = os.path.join(data_dir, file)
    path_out = os.path.join(data_dir, filename + '-resampled.wav')
    tfm = sox.Transformer()
    tfm.convert(samplerate=16000, n_channels=1, bitdepth=16)
    tfm.build(path_in, path_out)
    return path_out


def recognize_sync(file):
    path = os.path.join(data_dir, file)
    with open(path, 'rb') as f:
        content = base64.b64encode(f.read())
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
    return response['results'][0]['alternatives'][0]['transcript']


def recognize_async(file):
    path = os.path.join(data_dir, file)
    with open(path, 'rb') as f:
        content = base64.b64encode(f.read())
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
    request = speech.asyncrecognize(body=request_body)
    response = request.execute()
    operation_id = response['name']
    for retries in range(100):
        operation = service.operations().get(name=operation_id).execute()
        if ('done' in operation.keys()):
            async_response = operation['response']
            return async_response['results'][0]['alternatives'][0]['transcript']
        else:
            time.sleep(30)
