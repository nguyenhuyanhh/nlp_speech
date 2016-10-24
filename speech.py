import base64
import os
import time

from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import sox

# init path
cur_dir = os.path.dirname(os.path.realpath(__name__))
data_dir = os.path.join(cur_dir, 'data/')

# init credentials
API_KEY = 'AIzaSyC22qOuouVqsraoV6KzCHNAzdf3gWisOwc'
key_path = os.path.join(cur_dir, 'key.json')
scopes = ['https://www.googleapis.com/auth/cloud-platform']
credentials = ServiceAccountCredentials.from_json_keyfile_name(
    key_path, scopes=scopes)

# init google apis
speech_service = build('speech', 'v1beta1', credentials=credentials)
speech = speech_service.speech()
operations = speech_service.operations()
storage_service = build('storage', 'v1', credentials=credentials)
objects = storage_service.objects()
bucket_name = 'speech-recognition-146903.appspot.com'


class Speech():

    def __init__(self, file_id):
        # initialize object, get paths programmatically
        self.file_id = file_id
        self.working_dir = os.path.join(data_dir, self.file_id)
        self.raw_dir = os.path.join(self.working_dir, 'raw/')
        raw_file = [f for f in os.listdir(
            self.raw_dir) if os.path.isfile(os.path.join(self.raw_dir, f))][0]
        self.raw_file = os.path.join(self.raw_dir, raw_file)
        self.resampled_dir = os.path.join(self.working_dir, 'resampled/')
        self.resampled_file = os.path.join(
            self.resampled_dir, self.file_id + '-resampled.wav')
        self.googleapi_dir = os.path.join(self.working_dir, 'googleapi/')
        self.googleapi_trans = os.path.join(
            self.googleapi_dir, self.file_id + '-transcript.txt')
        self.async_max_retries = 10
        self.async_retry_interval = 30

    def convert(self):
        """Resample file_id to 16kHz, 1 channel, 16 bit wav."""
        tfm = sox.Transformer()
        tfm.convert(samplerate=16000, n_channels=1, bitdepth=16)
        tfm.build(self.raw_file, self.resampled_file)

    def get_initial_wait(self):
        """
        Get initial wait for async response.
        Utilising an assumption that time taken to process a speech file
        is roughly the same as the duration of that file.
        """
        return os.path.getsize(self.resampled_file) / 32000

    def upload(self):
        """Upload resampled file to Google Cloud Storage."""
        request_body = {
            'name': self.file_id,
        }
        objects.insert(bucket=bucket_name, body=request_body,
                       media_body=self.resampled_file).execute()

    def recognize_sync(self):
        """
        For files shorter than one minute.
        Synchronously recognize file_id. Return transcript of resampled file.
        """
        # construct json request
        with open(self.resampled_file, 'rb') as f:
            content = base64.b64encode(f.read()).decode('utf-8')
        request_body = {
            "audio": {
                "content": content
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
        with open(self.googleapi_trans, 'w') as w:
            for item in result_list:
                w.write(item['alternatives'][0]['transcript'] + '\n')

    def recognize_async(self):
        """
        For files longer than one minute and up to 80 minutes.
        Asynchronously recognize file_id. Return transcript of resampled file.
        """
        # construct json request
        uri = 'gs://{}/{}'.format(bucket_name, self.file_id)
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
        time.sleep(self.get_initial_wait())
        for retries in range(self.async_max_retries):
            operation = operations.get(name=operation_id).execute()
            if ('done' in operation.keys()):
                async_response = operation['response']
                result_list = async_response['results']
                with open(self.googleapi_trans, 'w') as w:
                    for item in result_list:
                        w.write(item['alternatives'][0]['transcript'] + '\n')
                return
            else:
                time.sleep(self.async_retry_interval)


def list_id():
    """
    Return a list of file_ids to process (basically the subfolders of data_dir).
    """
    return [file_id for file_id in os.listdir(data_dir)
            if os.path.isdir(os.path.join(data_dir, file_id))]


def sync_pipeline(file_id):
    """Synchronous processing pipeline for file_id."""
    s = Speech(file_id)
    for method in [s.convert, s.recognize_sync]:
        method()


def async_pipeline(file_id):
    """Asynchronous processing pipeline for file_id."""
    s = Speech(file_id)
    for method in [s.convert, s.upload, s.recognize_async]:
        method()
