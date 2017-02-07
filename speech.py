"""Speech operations."""

import base64
import json
import logging
import os
import random
import subprocess
import sys
import time
import wave
from decimal import Decimal

import sox
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

# initialize paths
CUR_DIR = os.path.dirname(os.path.realpath(__name__))
DATA_DIR = os.path.join(CUR_DIR, 'data/')
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)
LIUM_PATH = os.path.join(CUR_DIR, 'lium/LIUM_SpkDiarization-8.4.1.jar')

# initialize credentials and google apis
with open(os.path.join(CUR_DIR, 'auth/api.json'), 'r') as api_:
    API_SPEC = json.load(api_)
    API_KEY = API_SPEC['api_key']
    BUCKET_NAME = API_SPEC['bucket_name']
JSON_KEY = os.path.join(CUR_DIR, 'auth/key.json')
CREDENTIALS = ServiceAccountCredentials.from_json_keyfile_name(
    JSON_KEY, scopes=['https://www.googleapis.com/auth/cloud-platform'])
SPEECH_SERVICE = build('speech', 'v1beta1', credentials=CREDENTIALS)
SPEECH = SPEECH_SERVICE.speech()
OPERATIONS = SPEECH_SERVICE.operations()
OP_BASE_URL = 'https://speech.googleapis.com/v1beta1/operations/'
STORAGE_SERVICE = build('storage', 'v1', credentials=CREDENTIALS)
OBJECTS = STORAGE_SERVICE.objects()

# initialize and silence loggers
LOG_FILE = logging.FileHandler('speech.log')
LOG = logging.getLogger(__name__)
LOG.addHandler(LOG_FILE)
logging.getLogger().disabled = True
logging.getLogger('oauth2client').setLevel(logging.ERROR)
logging.getLogger('googleapiclient').setLevel(logging.ERROR)


class Speech():
    """
    Speech operations on one file_id.
    Syntax: Speech(file_id)
    """

    def __init__(self, file_id):
        # initialize object, get paths programmatically
        self.file_id = file_id
        self.working_dir = os.path.join(DATA_DIR, self.file_id)
        self.raw_dir = os.path.join(self.working_dir, 'raw/')
        raw_file = [f for f in os.listdir(
            self.raw_dir) if os.path.isfile(os.path.join(self.raw_dir, f))][0]
        self.raw_file = os.path.join(self.raw_dir, raw_file)
        self.resampled_dir = os.path.join(self.working_dir, 'resampled/')
        self.resampled_file = os.path.join(
            self.resampled_dir, self.file_id + '.wav')
        self.diarize_dir = os.path.join(self.working_dir, 'diarization/')
        self.diarize_file = os.path.join(
            self.diarize_dir, self.file_id + '.seg')
        self.trans_dir = os.path.join(self.working_dir, 'transcript/')
        self.googleapi_dir = os.path.join(self.trans_dir, 'googleapi/')
        self.textgrid_dir = os.path.join(self.trans_dir, 'textgrid/')
        self.trans_sync = os.path.join(
            self.googleapi_dir, self.file_id + '-sync.txt')
        self.trans_async = os.path.join(
            self.googleapi_dir, self.file_id + '-async.txt')
        self.trans_diarize = os.path.join(
            self.googleapi_dir, self.file_id + '.txt')
        self.textgrid = os.path.join(
            self.textgrid_dir, self.file_id + '.TextGrid')
        self.temp_dir = os.path.join(self.working_dir, 'temp/')
        if not os.path.exists(self.temp_dir):
            os.makedirs(self.temp_dir)
        self.temp_seg_to_dict = os.path.join(self.temp_dir, 'seg_to_dict.json')
        self.temp_dict_to_wav = os.path.join(self.temp_dir, 'dict_to_wav.json')
        self.temp_wav_to_trans = os.path.join(
            self.temp_dir, 'wav_to_trans.json')
        self.async_max_retries = 10
        self.async_retry_interval = 30

    def has_raw(self):
        """Check for raw file."""
        return os.path.exists(self.raw_file)

    def has_resampled(self):
        """Check for resampled file."""
        return os.path.exists(self.resampled_file)

    def has_diarize(self):
        """Check for diarized file."""
        return os.path.exists(self.diarize_file)

    def has_temp_seg_to_dict(self):
        """Check for temporary seg_to_dict.json."""
        return os.path.exists(self.temp_seg_to_dict)

    def has_temp_dict_to_wav(self):
        """Check for temporary dict_to_wav.json."""
        return os.path.exists(self.temp_dict_to_wav)

    def has_temp_wav_to_trans(self):
        """Check for temporary wav_to_trans.json."""
        return os.path.exists(self.temp_wav_to_trans)

    def has_trans_sync(self):
        """Check for synchronous transcription."""
        return os.path.exists(self.trans_sync)

    def has_trans_async(self):
        """Check for asynchronous transcription."""
        return os.path.exists(self.trans_async)

    def has_trans_diarize(self):
        """Check for combined transcription from diarization."""
        return os.path.exists(self.trans_diarize)

    def has_textgrid(self):
        """Check for TextGrid file."""
        return os.path.exists(self.textgrid)

    def get_duration(self):
        """Return duration of a wav file."""
        file_ = wave.open(self.resampled_file, 'r')
        duration = Decimal(file_.getnframes()) / file_.getframerate()
        file_.close()
        return duration

    def convert(self):
        """Resample file_id to 16kHz, 1 channel, 16 bit wav."""
        tfm = sox.Transformer()
        tfm.convert(samplerate=16000, n_channels=1, bitdepth=16)
        tfm.build(self.raw_file, self.resampled_file)
        LOG.info('convert: %s: Resampled file written.', self.file_id)

    def upload(self):
        """Upload resampled file to Google Cloud Storage."""
        request_body = {
            'name': self.file_id,
        }
        OBJECTS.insert(bucket=BUCKET_NAME, body=request_body,
                       media_body=self.resampled_file).execute()
        LOG.info('upload: %s: File uploaded.', self.file_id)

    def diarize(self):
        """LIUM diarization of file_id."""
        # silent output
        fnull = open(os.devnull, 'w')
        args = ['java', '-Xmx2048m', '-jar', LIUM_PATH, '--fInputMask=' +
                self.resampled_file, '--sOutputMask=' + self.diarize_file,
                '--doCEClustering', self.file_id]
        subprocess.call(args, stdout=fnull, stderr=subprocess.STDOUT)
        if self.has_diarize():
            LOG.info('diarize: %s: Diarization file written.', self.file_id)
        else:  # likely that resampled file is corrupted
            self.convert()
            self.diarize()

    def seg_to_dict(self):
        """Convert LIUM output to Python-friendly input."""
        diarize_dict = dict()
        with open(self.diarize_file, 'r') as file_:
            line_list = file_.readlines()
            for line in line_list:
                words = line.strip().split()
                if words[0] == self.file_id:
                    speaker_gender = words[7] + '-' + words[4]
                    start_time = Decimal(words[2]) / 100
                    end_time = (Decimal(words[2]) + Decimal(words[3])) / 100
                    diarize_dict[int(words[2])] = (
                        speaker_gender, str(start_time), str(end_time))
        with open(self.temp_seg_to_dict, 'w') as file_out:
            json.dump(diarize_dict, file_out, sort_keys=True, indent=4)
        LOG.info('seg_to_dict: %s: Completed.', self.file_id)

    def split_resampled(self):
        """Split resampled file according to LIUM output."""
        count = 1
        with open(self.temp_seg_to_dict, 'r') as file_:
            diarize_dict = json.load(file_)
        sorted_keys = sorted([int(x) for x in diarize_dict.keys()])
        for key in sorted_keys:
            value = diarize_dict[str(key)]
            diar_part_filename = '{}-{}.wav'.format(count, value[0])
            diar_part_path = os.path.join(self.diarize_dir, diar_part_filename)
            tfm = sox.Transformer()
            tfm.trim(Decimal(value[1]), Decimal(value[2]))
            tfm.build(self.resampled_file, diar_part_path)
            diarize_dict[str(key)] = (
                value[0], value[1], value[2], diar_part_path)
            count += 1
            LOG.info('split_resampled: Done with key %s', key)
        with open(self.temp_dict_to_wav, 'w') as file_out:
            json.dump(diarize_dict, file_out, sort_keys=True, indent=4)
        LOG.info('split_resampled: %s: Completed.', self.file_id)

    def recognize_diarize(self):
        """Synchronously recognize diarized parts of file_id."""
        with open(self.temp_dict_to_wav, 'r') as file_:
            diarize_dict = json.load(file_)
        sorted_keys = sorted([int(x) for x in diarize_dict.keys()])
        for key in sorted_keys:
            value = diarize_dict[str(key)]
            diar_part_path = value[3]
            with open(diar_part_path, 'rb') as file_:
                content = base64.b64encode(file_.read()).decode('utf-8')
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

            # exponential backoff in case it fails
            attempt = 1
            while attempt <= 5:
                try:
                    sync_response = SPEECH.syncrecognize(
                        body=request_body).execute()
                    break
                except:
                    time.sleep(2**attempt + random.randint(0, 1000) / 1000)
                    attempt += 1
                    LOG.info('recognize_diarize: Retrying key %s', key)

            if attempt == 6:
                LOG.info(
                    'recognize_diarize: Failed to transcribe key %s', key)
                new_value = (value[0], value[1], value[2], '')
            elif 'results' not in sync_response.keys():
                new_value = (value[0], value[1], value[2], '')
            else:
                result_list = sync_response['results']
                trans_list = list()
                for item in result_list:
                    trans_list.append(item['alternatives'][0]['transcript'])
                result_str = ' '.join(trans_list)
                new_value = (value[0], value[1], value[2],
                             result_str.encode('utf-8'))
            diarize_dict[str(key)] = new_value
            LOG.info('recognize_diarize: Done with key %s', key)
        with open(self.temp_wav_to_trans, 'w') as file_out:
            json.dump(diarize_dict, file_out, sort_keys=True, indent=4)
        LOG.info('recognize_diarize: %s: Completed.', self.file_id)

    def write_transcript(self):
        """Write back transcript and TextGrid for file_id."""
        with open(self.temp_wav_to_trans, 'r') as file_:
            diarize_dict = json.load(file_)
        sorted_keys = sorted([int(x) for x in diarize_dict.keys()])
        intervals = len(diarize_dict)
        duration = self.get_duration()

        # write back transcript
        with open(self.trans_diarize, 'w') as file_out:
            for key in sorted_keys:
                value = diarize_dict[str(key)]
                file_out.write(value[3].encode('utf-8') + '\n')
            LOG.info(
                'write_transcript: %s: Transcript written.', self.file_id)

        # write back textgrid
        with open(self.textgrid, 'w') as file_out:
            # header
            file_out.write('File type = "ooTextFile"\n')
            file_out.write('Object class = "TextGrid"\n\n')
            file_out.write('xmin = 0.0\nxmax = {}\n'.format(duration))
            file_out.write('tiers? <exists>\nsize = 1\nitem []:\n')
            file_out.write('    item[1]:\n        class = "IntervalTier"\n')
            file_out.write('        name = "default"\n')
            file_out.write('        xmin = 0.0\n')
            file_out.write('        xmax = {}\n'.format(duration))
            file_out.write('        intervals: size = {}\n'.format(intervals))
            # items
            count = 1
            for key in sorted_keys:
                value = diarize_dict[str(key)]
                file_out.write('        intervals [{}]\n'.format(count))
                file_out.write('            xmin = {}\n'.format(value[1]))
                file_out.write('            xmax = {}\n'.format(value[2]))
                file_out.write('            text = "{}"\n'.format(
                    value[3].encode('utf-8')))
                count += 1
            LOG.info(
                'write_transcript: %s: TextGrid written.', self.file_id)

    def recognize_sync(self):
        """
        For files shorter than one minute.
        Synchronously recognize file_id. Return transcript of resampled file.
        """
        # construct json request
        with open(self.resampled_file, 'rb') as file_:
            content = base64.b64encode(file_.read()).decode('utf-8')
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
        sync_response = SPEECH.syncrecognize(body=request_body).execute()

        # write back transcript if present
        if 'results' not in sync_response.keys():
            LOG.info(
                'recognize_sync: %s: No transcript returned.', self.file_id)
        else:
            result_list = sync_response['results']
            with open(self.trans_sync, 'w') as file_out:
                for item in result_list:
                    file_out.write(item['alternatives'][0]
                                   ['transcript'] + '\n')
            LOG.info(
                'recognize_sync: %s: Transcript written.', self.file_id)

    def recognize_async(self):
        """
        For files longer than one minute and up to 80 minutes.
        Asynchronously recognize file_id. Return transcript of resampled file.
        """
        # construct json request
        uri = 'gs://{}/{}'.format(BUCKET_NAME, self.file_id)
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
        async_response = SPEECH.asyncrecognize(body=request_body).execute()
        operation_id = async_response['name']
        LOG.info('recognize_async: %s', self.file_id)
        LOG.info('Request URL: %s%s?alt=json&key=%s',
                 OP_BASE_URL, operation_id, API_KEY)

        # periodically poll for response up until a limit
        # if there is, write back to file
        time.sleep(self.get_duration())
        for _ in range(self.async_max_retries):
            operation = OPERATIONS.get(name=operation_id).execute()
            if 'done' in operation.keys():
                async_response = operation['response']
                result_list = async_response['results']
                with open(self.trans_async, 'w') as file_out:
                    for item in result_list:
                        file_out.write(item['alternatives'][0]
                                       ['transcript'] + '\n')
                LOG.info(
                    'recognize_async: %s: Transcript written.', self.file_id)
                return self.file_id
            else:
                time.sleep(self.async_retry_interval)


def sync_pipeline(file_id):
    """Synchronous processing pipeline for file_id."""
    speech_ = Speech(file_id)

    # check for completion
    # if not start the process
    if speech_.has_trans_sync():
        LOG.info('recognize_sync: %s: Transcript exists.', file_id)
        return file_id

    # convert, check for raw and resampled
    if not speech_.has_raw():
        LOG.info('convert: %s: Raw file does not exist. Exiting.', file_id)
        return None
    elif speech_.has_resampled():
        LOG.info('convert: %s: Resampled file exists.', file_id)
    else:
        speech_.convert()

    # recognize_sync, check for duration
    if speech_.get_duration() >= 60:
        LOG.info(
            'recognize_sync: %s: File longer than 1 minute. Exiting.', file_id)
        return None
    else:
        speech_.recognize_sync()

    return file_id


def async_pipeline(file_id):
    """Asynchronous processing pipeline for file_id."""
    speech_ = Speech(file_id)

    # check for completion
    # if not start the process
    if speech_.has_trans_async():
        LOG.info('recognize_async: %s: Transcript exists.', file_id)
        return file_id

    # convert, check for raw and resampled
    if not speech_.has_raw():
        LOG.info('convert: %s: Raw file does not exist. Exiting.', file_id)
        return None
    elif speech_.has_resampled():
        LOG.info('convert: %s: Resampled file exists.', file_id)
    else:
        speech_.convert()

    # upload, check for duration
    if speech_.get_duration() >= 4800:
        LOG.info('upload: %s: File longer than 1 minute. Exiting.', file_id)
        return None
    else:
        speech_.upload()

    # recognize_async
    speech_.recognize_async()

    return file_id


def diarize_pipeline(file_id):
    """Synchronous processing pipeline with diarization for file_id."""
    speech_ = Speech(file_id)

    # check for completion
    # if not start the process
    if speech_.has_trans_diarize() and speech_.has_textgrid():
        LOG.info(
            'write_transcript: %s: Transcript and TextGrid exists.', file_id)
        return file_id

    # convert, check for raw and resampled
    if not speech_.has_raw():
        LOG.info('convert: %s: Raw file does not exist. Exiting.', file_id)
        return None
    elif speech_.has_resampled():
        LOG.info('convert: %s: Resampled file exists.', file_id)
    else:
        speech_.convert()

    # diarize, check for seg
    if speech_.has_diarize():
        LOG.info('diarize: %s: Diarization file exists.', file_id)
    else:
        speech_.diarize()

    # seg_to_dict, check for temp
    if speech_.has_temp_seg_to_dict():
        LOG.info('seg_to_dict: %s: Previously completed.', file_id)
    else:
        speech_.seg_to_dict()

    # split_resampled, check for temp
    if speech_.has_temp_dict_to_wav():
        LOG.info('split_resampled: %s: Previously completed.', file_id)
    else:
        speech_.split_resampled()

    # recognize_diarize, check for temp
    if speech_.has_temp_wav_to_trans():
        LOG.info('recognize_diarize: %s: Previously completed.', file_id)
    else:
        speech_.recognize_diarize()

    # write_transcript
    speech_.write_transcript()

    return file_id


def workflow(method='diarize'):
    """Workflow for /data."""
    id_list = sorted([file_id for file_id in os.listdir(DATA_DIR)
                      if os.path.isdir(os.path.join(DATA_DIR, file_id))])
    if method not in ['diarize', 'sync', 'async']:
        LOG.info('Invalid workflow method. Exiting.')
        return
    elif method == 'diarize':
        for file_id in id_list:
            try:
                diarize_pipeline(file_id)
            except:
                LOG.error('diarize_pipeline: %s: Error occured.',
                          file_id, exc_info=1)
                continue
    elif method == 'sync':
        for file_id in id_list:
            try:
                sync_pipeline(file_id)
            except:
                LOG.error('sync_pipeline: %s: Error occured.',
                          file_id, exc_info=1)
                continue
    else:
        for file_id in id_list:
            try:
                async_pipeline(file_id)
            except:
                LOG.error('async_pipeline: %s: Error occured.',
                          file_id, exc_info=1)
                continue
    LOG.info('Workflow completed.')

if __name__ == '__main__':
    if len(sys.argv) == 1 or sys.argv[1] in ['-d', '--default', '--diarize']:
        workflow(method='diarize')
    elif sys.argv[1] in ['-s', '--sync']:
        workflow(method='sync')
    elif sys.argv[1] in ['-a', '--async']:
        workflow(method='async')
    else:
        LOG.info('Invalid arguments. Exiting.')
