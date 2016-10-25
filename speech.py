import base64
import os
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import cpu_count

from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import sox

# initialize paths
cur_dir = os.path.dirname(os.path.realpath(__name__))
data_dir = os.path.join(cur_dir, 'data/')

# initialize credentials
API_KEY = 'AIzaSyC22qOuouVqsraoV6KzCHNAzdf3gWisOwc'
key_path = os.path.join(cur_dir, 'key.json')
scopes = ['https://www.googleapis.com/auth/cloud-platform']
credentials = ServiceAccountCredentials.from_json_keyfile_name(
    key_path, scopes=scopes)

# initialize google apis
speech_service = build('speech', 'v1beta1', credentials=credentials)
speech = speech_service.speech()
operations = speech_service.operations()
storage_service = build('storage', 'v1', credentials=credentials)
objects = storage_service.objects()
bucket_name = 'speech-recognition-146903.appspot.com'

# initialize logger
logger = logging.getLogger(__name__)


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
        self.googleapi_trans_sync = os.path.join(
            self.googleapi_dir, self.file_id + '-transcript-sync.txt')
        self.googleapi_trans_async = os.path.join(
            self.googleapi_dir, self.file_id + '-transcript-async.txt')
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
        # check for length
        if (os.path.getsize(self.resampled_file) >= 1920000):
            logger.info(
                '%s: File longer than 1 minute. Will not recognize.', self.file_id)
            return None
        else:
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

            # write back transcript if present
            if ('results' not in sync_response.keys()):
                logger.info(
                    '%s: No results. Transcript not returned.', self.file_id)
            else:
                result_list = sync_response['results']
                with open(self.googleapi_trans_sync, 'w') as w:
                    for item in result_list:
                        w.write(item['alternatives'][0]['transcript'] + '\n')
                logger.info('%s: Transcript written.', self.file_id)

    def recognize_async(self):
        """
        For files longer than one minute and up to 80 minutes.
        Asynchronously recognize file_id. Return transcript of resampled file.
        """
        # check for length
        if (os.path.getsize(self.resampled_file) >= 153600000):
            return None
        else:
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
                if ('done' in operation.keys() & operation['done'] == True):
                    async_response = operation['response']
                    result_list = async_response['results']
                    with open(self.googleapi_trans_async, 'w') as w:
                        for item in result_list:
                            w.write(item['alternatives'][0]
                                    ['transcript'] + '\n')
                    return
                else:
                    time.sleep(self.async_retry_interval)


def sync_pipeline(file_id):
    """Synchronous processing pipeline for file_id."""
    s = Speech(file_id)
    for method in [s.convert, s.recognize_sync]:
        method()
    return file_id


def async_pipeline(file_id):
    """Asynchronous processing pipeline for file_id."""
    s = Speech(file_id)
    for method in [s.convert, s.upload, s.recognize_async]:
        method()
    return file_id


def workflow(method='async'):
    """Multi-processing workflow."""
    if method not in ['sync', 'async']:
        return None
    future_list = list()
    id_list = [file_id for file_id in os.listdir(
        data_dir) if os.path.isdir(os.path.join(data_dir, file_id))]
    max_workers = cpu_count() * 5
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for file_id in id_list:
            if (method == 'async'):
                future_list.append(executor.submit(async_pipeline, file_id))
            else:
                future_list.append(executor.submit(sync_pipeline, file_id))


def sync_workflow():
    id_list = [file_id for file_id in os.listdir(
        data_dir) if os.path.isdir(os.path.join(data_dir, file_id))]
    for file_id in id_list:
        sync_pipeline(file_id)
