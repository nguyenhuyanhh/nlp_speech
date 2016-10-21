import base64
import json
import os
import time

from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import sox

# inits
cur_dir = os.path.dirname(os.path.realpath(__name__))
data_dir = os.path.join(cur_dir, 'data/')
# API_KEY = 'AIzaSyC22qOuouVqsraoV6KzCHNAzdf3gWisOwc'
key_path = os.path.join(cur_dir, 'key.json')
scopes = ['https://www.googleapis.com/auth/cloud-platform']
credentials = ServiceAccountCredentials.from_json_keyfile_name(
    key_path, scopes=scopes)
speech_service = build('speech', 'v1beta1', credentials=credentials)
speech = speech_service.speech()
operations = speech_service.operations()
storage_service = build('storage', 'v1', credentials=credentials)
objects = storage_service.objects()
bucket_name = 'speech-recognition-146903.appspot.com'


def list_id():
    """
    Return a list of file_ids to process (basically the subfolders of data_dir).
    """
    return [file_id for file_id in os.listdir(data_dir)
            if os.path.isdir(os.path.join(data_dir, file_id))]


def convert(file_id):
    """
    Resample file_id to 16kHz, 1 channel, 16 bit wav.
    """
    # programmatically get path_in and path_out
    working_dir = os.path.join(data_dir, file_id)
    raw_dir = os.path.join(working_dir, 'raw/')
    resampled_dir = os.path.join(working_dir, 'resampled/')
    raw_file = [f for f in os.listdir(
        raw_dir) if os.path.isfile(os.path.join(raw_dir, f))][0]
    path_in = os.path.join(raw_dir, raw_file)
    path_out = os.path.join(resampled_dir, file_id + '-resampled.wav')

    # convert
    tfm = sox.Transformer()
    tfm.convert(samplerate=16000, n_channels=1, bitdepth=16)
    tfm.build(path_in, path_out)


def get_initial_wait(file_id):
    # programmatically get path
    working_dir = os.path.join(data_dir, file_id)
    resampled_dir = os.path.join(working_dir, 'resampled/')
    path = os.path.join(resampled_dir, file_id + '-resampled.wav')

    return os.path.getsize(path) / 32000


def upload(file_id):
    """
    Upload resampled file to Google Cloud Storage. Return URL to file.
    """
    # programmatically get path
    working_dir = os.path.join(data_dir, file_id)
    resampled_dir = os.path.join(working_dir, 'resampled/')
    path = os.path.join(resampled_dir, file_id + '-resampled.wav')

    # construct json request
    request_body = {
        'name': file_id,
    }
    response = objects.insert(bucket=bucket_name,
                              body=request_body, media_body=path).execute()


def recognize_sync(file_id):
    """
    For files shorter than one minute.
    Synchronously recognize file_id. Return transcript of resampled file.
    """
    # programmatically get path
    working_dir = os.path.join(data_dir, file_id)
    resampled_dir = os.path.join(working_dir, 'resampled/')
    googleapi_dir = os.path.join(working_dir, 'googleapi/')
    path = os.path.join(resampled_dir, file_id + '-resampled.wav')

    # construct json request
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
    sync_response = speech.syncrecognize(body=request_body).execute()

    # write back transcript
    result_list = sync_response['results']
    write_path = os.path.join(googleapi_dir, file_id + '-sync.txt')
    with open(write_path, 'w') as w:
        for item in result_list:
            w.write(item['alternatives'][0]['transcript'] + '\n')


def recognize_async(file_id, max_retries=10, retry_interval=30):
    """
    For files longer than one minute and up to 80 minutes.
    Synchronously recognize file_id. Return transcript of resampled file.
    """
    # programmatically get path
    working_dir = os.path.join(data_dir, file_id)
    googleapi_dir = os.path.join(working_dir, 'googleapi/')

    # construct json request
    uri = 'gs://{}/{}'.format(bucket_name, file_id)
    request_body = {
        "audio": {
            "uri": uri
        },
        "config": {
            "languageCode": "en-US",
            "encoding": "LINEAR16",
            "sampleRate": 16000
        },
    }
    async_response = speech.asyncrecognize(body=request_body).execute()
    operation_id = async_response['name']

    # periodically poll for response up until a limit
    # if there is, write back to file
    time.sleep(get_initial_wait(file_id))
    for retries in range(max_retries):
        operation = operations.get(name=operation_id).execute()
        if ('done' in operation.keys()):
            async_response = operation['response']
            result_list = async_response['results']
            write_path = os.path.join(googleapi_dir, file_id + '-async.txt')
            with open(write_path, 'w') as w:
                for item in result_list:
                    w.write(item['alternatives'][0]['transcript'] + '\n')
            return
        else:
            time.sleep(retry_interval)
