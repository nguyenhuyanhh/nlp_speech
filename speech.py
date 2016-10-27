import base64
import os
import time
import logging
import subprocess
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor, as_completed

from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import sox

# initialize paths
cur_dir = os.path.dirname(os.path.realpath(__name__))
data_dir = os.path.join(cur_dir, 'data/')
lium_dir = os.path.join(cur_dir, 'lium/')
lium_path = os.path.join(lium_dir, 'LIUM_SpkDiarization-8.4.1.jar')

# initialize credentials
api_key = 'AIzaSyC22qOuouVqsraoV6KzCHNAzdf3gWisOwc'
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
logger.setLevel(logging.INFO)
logger_oauth = logging.getLogger('oauth2client')
logger_oauth.setLevel(logging.ERROR)


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
        self.diarize_dir = os.path.join(self.working_dir, 'diarization/')
        self.diarize_out = os.path.join(
            self.diarize_dir, self.file_id + '-diarize.seg')
        self.googleapi_dir = os.path.join(self.working_dir, 'googleapi/')
        self.googleapi_trans_sync = os.path.join(
            self.googleapi_dir, self.file_id + '-transcript-sync.txt')
        self.googleapi_trans_async = os.path.join(
            self.googleapi_dir, self.file_id + '-transcript-async.txt')
        self.async_max_retries = 10
        self.async_retry_interval = 30

    def has_raw(self):
        return os.path.exists(self.raw_file)

    def has_resampled(self):
        return os.path.exists(self.resampled_file)

    def has_trans_sync(self):
        return os.path.exists(self.googleapi_trans_sync)

    def has_trans_async(self):
        return os.path.exists(self.googleapi_trans_async)

    def get_duration(self):
        return os.path.getsize(self.resampled_file) / 32000

    def convert(self):
        """Resample file_id to 16kHz, 1 channel, 16 bit wav."""
        tfm = sox.Transformer()
        tfm.convert(samplerate=16000, n_channels=1, bitdepth=16)
        tfm.build(self.raw_file, self.resampled_file)
        logger.info('%s: Resampled file written.', self.file_id)

    def diarize(self):
        """LIUM diarization of file_id."""
        # call lium
        args = ['java', '-Xmx1024m', '-jar', lium_path, '--fInputMask=' +
                self.resampled_file, '--sOutputMask=' + self.diarize_out, self.file_id]
        subprocess.call(args)

        # diarization specification
        diarize_dict = dict()
        with open(self.diarize_out, 'r') as file:
            line_list = file.readlines()
            for line in line_list:
                words = line.strip().split()
                if (words[0] == self.file_id):
                    speaker_gender = words[7] + '-' + words[4]
                    start_time = Decimal(words[2]) / 100
                    end_time = (Decimal(words[2]) + Decimal(words[3])) / 100
                    diarize_dict[int(words[2])] = (
                        speaker_gender, start_time, end_time)

        # split resampled file according to diarization specs
        # then update the specs
        count = 1
        for key in sorted(diarize_dict.keys()):
            value = diarize_dict[key]
            file_name = '{}-{}.wav'.format(count, value[0])
            path_out = os.path.join(self.diarize_dir, file_name)
            tfm = sox.Transformer()
            tfm.trim(value[1], value[2])
            tfm.build(self.resampled_file, path_out)
            new_value = (value[0], value[1], value[2], file_name)
            diarize_dict[key] = new_value
            count += 1

        return diarize_dict

    def upload(self):
        """Upload resampled file to Google Cloud Storage."""
        request_body = {
            'name': self.file_id,
        }
        objects.insert(bucket=bucket_name, body=request_body,
                       media_body=self.resampled_file).execute()
        logger.info('%s: File uploaded.', self.file_id)

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
        logger.info('%s: Request URL: https://speech.googleapis.com/v1beta1/operations/%s?alt=json&key=%s',
                    self.file_id, operation_id, api_key)

        # periodically poll for response up until a limit
        # if there is, write back to file
        time.sleep(self.get_duration())
        for retries in range(self.async_max_retries):
            operation = operations.get(name=operation_id).execute()
            if ('done' in operation.keys()):
                async_response = operation['response']
                result_list = async_response['results']
                with open(self.googleapi_trans_async, 'w') as w:
                    for item in result_list:
                        w.write(item['alternatives'][0]
                                ['transcript'] + '\n')
                logger.info('%s: Transcript written.', self.file_id)
                return self.file_id
            else:
                time.sleep(self.async_retry_interval)


def sync_pipeline(file_id):
    """Synchronous processing pipeline for file_id."""
    s = Speech(file_id)

    # convert, check for raw and resampled
    if (not s.has_raw()):
        logger.info(
            '%s: Raw file does not exist. No further action.', file_id)
    elif (s.has_resampled()):
        logger.info(
            '%s: Resampled file exists. No further action.', file_id)
    else:
        s.convert()

    # recognize_sync, check for duration and transcript
    if (s.get_duration() >= 60):
        logger.info(
            '%s: File longer than 1 minute. Will not recognize.', file_id)
        return None
    elif (s.has_trans_sync()):
        logger.info(
            '%s: Transcript exists. No further action.', file_id)
    else:
        s.recognize_sync()

    return file_id


def async_pipeline(file_id):
    """Asynchronous processing pipeline for file_id."""
    s = Speech(file_id)

    # convert, check for raw and resampled
    if (not s.has_raw()):
        logger.info(
            '%s: Raw file does not exist. No further action.', file_id)
    elif (s.has_resampled()):
        logger.info(
            '%s: Resampled file exists. No further action.', file_id)
    else:
        s.convert()

    # upload, check for duration
    if (s.get_duration() >= 4800):
        logger.info(
            '%s: File longer than 1 minute. Will not recognize.', file_id)
        return None
    else:
        s.upload()

    # recognize_async, check for transcript
    if (s.has_trans_async()):
        logger.info(
            '%s: Transcript exists. No further action.', file_id)
    else:
        s.recognize_async()

    return file_id


def sync_workflow():
    """Synchronous processing workflow for /data."""
    id_list = [file_id for file_id in os.listdir(
        data_dir) if os.path.isdir(os.path.join(data_dir, file_id))]
    for file_id in id_list:
        sync_pipeline(file_id)


def async_workflow(max_workers=20):
    """Asynchronous multi-processing workflow for /data."""
    future_list = list()
    id_list = [file_id for file_id in os.listdir(
        data_dir) if os.path.isdir(os.path.join(data_dir, file_id))]
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for file_id in id_list:
            future_list.append(executor.submit(async_pipeline, file_id))
