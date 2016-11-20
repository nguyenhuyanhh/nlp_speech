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

# initialize and silence loggers
logger = logging.getLogger(__name__)
logger_sox = logging.getLogger()
logger_sox.disabled = True
logger_oauth = logging.getLogger('oauth2client')
logger_oauth.setLevel(logging.ERROR)
logger_googleapi = logging.getLogger('googleapiclient')
logger_googleapi.setLevel(logging.ERROR)


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
        return os.path.exists(self.raw_file)

    def has_resampled(self):
        return os.path.exists(self.resampled_file)

    def has_diarize(self):
        return os.path.exists(self.diarize_file)

    def has_temp_seg_to_dict(self):
        return os.path.exists(self.temp_seg_to_dict)

    def has_temp_dict_to_wav(self):
        return os.path.exists(self.temp_dict_to_wav)

    def has_temp_wav_to_trans(self):
        return os.path.exists(self.temp_wav_to_trans)

    def has_trans_sync(self):
        return os.path.exists(self.trans_sync)

    def has_trans_async(self):
        return os.path.exists(self.trans_async)

    def has_trans_diarize(self):
        return os.path.exists(self.trans_diarize)

    def has_textgrid(self):
        return os.path.exists(self.textgrid)

    def get_duration(self):
        f = wave.open(self.resampled_file, 'r')
        duration = Decimal(f.getnframes()) / f.getframerate()
        f.close()
        return duration

    def convert(self):
        """Resample file_id to 16kHz, 1 channel, 16 bit wav."""
        tfm = sox.Transformer()
        tfm.convert(samplerate=16000, n_channels=1, bitdepth=16)
        tfm.build(self.raw_file, self.resampled_file)
        logger.info('convert: %s: Resampled file written.', self.file_id)

    def upload(self):
        """Upload resampled file to Google Cloud Storage."""
        request_body = {
            'name': self.file_id,
        }
        objects.insert(bucket=bucket_name, body=request_body,
                       media_body=self.resampled_file).execute()
        logger.info('upload: %s: File uploaded.', self.file_id)

    def diarize(self):
        """LIUM diarization of file_id."""
        # silent output
        fnull = open(os.devnull, 'w')
        args = ['java', '-Xmx2048m', '-jar', lium_path, '--fInputMask=' +
                self.resampled_file, '--sOutputMask=' + self.diarize_file, self.file_id]
        subprocess.call(args, stdout=fnull, stderr=subprocess.STDOUT)
        if self.has_diarize():
            logger.info('diarize: %s: Diarization file written.', self.file_id)
        else:  # likely that resampled file is corrupted
            self.convert()
            self.diarize()

    def seg_to_dict(self):
        """Convert LIUM output to Python-friendly input."""
        diarize_dict = dict()
        with open(self.diarize_file, 'r') as file:
            line_list = file.readlines()
            for line in line_list:
                words = line.strip().split()
                if (words[0] == self.file_id):
                    speaker_gender = words[7] + '-' + words[4]
                    start_time = Decimal(words[2]) / 100
                    end_time = (Decimal(words[2]) + Decimal(words[3])) / 100
                    diarize_dict[int(words[2])] = (
                        speaker_gender, str(start_time), str(end_time))
        with open(self.temp_seg_to_dict, 'w') as w:
            json.dump(diarize_dict, w, sort_keys=True, indent=4)
        logger.info('seg_to_dict: %s: Completed.', self.file_id)

    def split_resampled(self):
        """Split resampled file according to LIUM output."""
        count = 1
        with open(self.temp_seg_to_dict, 'r') as f:
            diarize_dict = json.load(f)
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
            logger.info('split_resampled: Done with key %s', key)
        with open(self.temp_dict_to_wav, 'w') as w:
            json.dump(diarize_dict, w, sort_keys=True, indent=4)
        logger.info('split_resampled: %s: Completed.', self.file_id)

    def recognize_diarize(self):
        """Synchronously recognize diarized parts of file_id."""
        with open(self.temp_dict_to_wav, 'r') as f:
            diarize_dict = json.load(f)
        sorted_keys = sorted([int(x) for x in diarize_dict.keys()])
        for key in sorted_keys:
            value = diarize_dict[str(key)]
            diar_part_path = value[3]
            with open(diar_part_path, 'rb') as f:
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

            # exponential backoff in case it fails
            attempt = 1
            while attempt <= 5:
                try:
                    sync_response = speech.syncrecognize(
                        body=request_body).execute()
                    break
                except:
                    time.sleep(2**attempt + random.randint(0, 1000) / 1000)
                    attempt += 1
                    logger.info('recognize_diarize: Retrying key &s', key)
            if attempt == 6:
                logger.info(
                    'recognize_diarize: Failed to acquire transcription for key %s after 5 attempts', key)
                new_value = (value[0], value[1], value[2], '')

            if ('results' not in sync_response.keys()):
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
            logger.info('recognize_diarize: Done with key %s', key)
        with open(self.temp_wav_to_trans, 'w') as w:
            json.dump(diarize_dict, w, sort_keys=True, indent=4)
        logger.info('recognize_diarize: %s: Completed.', self.file_id)

    def write_transcript(self):
        """Write back transcript and TextGrid for file_id."""
        with open(self.temp_wav_to_trans, 'r') as f:
            diarize_dict = json.load(f)
        sorted_keys = sorted([int(x) for x in diarize_dict.keys()])

        # write back transcript
        with open(self.trans_diarize, 'w') as w:
            for key in sorted_keys:
                value = diarize_dict[str(key)]
                w.write(value[3] + '\n')
            logger.info(
                'write_transcript: %s: Transcript written.', self.file_id)

        # write back textgrid
        with open(self.textgrid, 'w') as w:
            # header
            w.write('File type = "ooTextFile"\nObject class = "TextGrid"\n\n')
            w.write('xmin = 0.0\nxmax = {}\ntiers? <exists>\nsize = 1\n'.format(
                self.get_duration()))
            w.write('item []:\n    item[1]:\n        class = "IntervalTier"\n')
            w.write('        name = "default"\n        xmin = 0.0\n')
            w.write('        xmax = {}\n        intervals: size = {}\n'.format(
                self.get_duration(), len(diarize_dict)))
            # items
            count = 1
            for key in sorted_keys:
                value = diarize_dict[str(key)]
                w.write('        intervals [{}]\n'.format(count))
                w.write('            xmin = {}\n'.format(value[1]))
                w.write('            xmax = {}\n'.format(value[2]))
                w.write('            text = "{}"\n'.format(value[3]))
                count += 1
            logger.info(
                'write_transcript: %s: TextGrid written.', self.file_id)

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
                'recognize_sync: %s: No results. Transcript not returned.', self.file_id)
        else:
            result_list = sync_response['results']
            with open(self.trans_sync, 'w') as w:
                for item in result_list:
                    w.write(item['alternatives'][0]['transcript'] + '\n')
            logger.info(
                'recognize_sync: %s: Transcript written.', self.file_id)

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
        logger.info('recognize_async: %s: Request URL: https://speech.googleapis.com/v1beta1/operations/%s?alt=json&key=%s',
                    self.file_id, operation_id, api_key)

        # periodically poll for response up until a limit
        # if there is, write back to file
        time.sleep(self.get_duration())
        for retries in range(self.async_max_retries):
            operation = operations.get(name=operation_id).execute()
            if ('done' in operation.keys()):
                async_response = operation['response']
                result_list = async_response['results']
                with open(self.trans_async, 'w') as w:
                    for item in result_list:
                        w.write(item['alternatives'][0]
                                ['transcript'] + '\n')
                logger.info(
                    'recognize_async: %s: Transcript written.', self.file_id)
                return self.file_id
            else:
                time.sleep(self.async_retry_interval)


def sync_pipeline(file_id):
    """Synchronous processing pipeline for file_id."""
    s = Speech(file_id)

    # convert, check for raw and resampled
    if (not s.has_raw()):
        logger.info(
            'convert: %s: Raw file does not exist. No further action.', file_id)
        return None
    elif (s.has_resampled()):
        logger.info(
            'convert: %s: Resampled file exists. No further action.', file_id)
    else:
        s.convert()

    # recognize_sync, check for duration and transcript
    if (s.get_duration() >= 60):
        logger.info(
            'recognize_sync: %s: File longer than 1 minute. Will not recognize.', file_id)
        return None
    elif (s.has_trans_sync()):
        logger.info(
            'recognize_sync: %s: Transcript exists. No further action.', file_id)
    else:
        s.recognize_sync()

    return file_id


def async_pipeline(file_id):
    """Asynchronous processing pipeline for file_id."""
    s = Speech(file_id)

    # convert, check for raw and resampled
    if (not s.has_raw()):
        logger.info(
            'convert: %s: Raw file does not exist. No further action.', file_id)
        return None
    elif (s.has_resampled()):
        logger.info(
            'convert: %s: Resampled file exists. No further action.', file_id)
    else:
        s.convert()

    # upload, check for duration
    if (s.get_duration() >= 4800):
        logger.info(
            'upload: %s: File longer than 1 minute. Will not recognize.', file_id)
        return None
    else:
        s.upload()

    # recognize_async, check for transcript
    if (s.has_trans_async()):
        logger.info(
            'recognize_async: %s: Transcript exists. No further action.', file_id)
    else:
        s.recognize_async()

    return file_id


def diarize_pipeline(file_id):
    """Synchronous processing pipeline with diarization for file_id."""
    s = Speech(file_id)

    # check for completion
    # if not start the process
    if (s.has_trans_diarize() and s.has_textgrid()):
        logger.info(
            'write_transcript: %s: Transcript and TextGrid exists. No further action.', file_id)
        return file_id

    # convert, check for raw and resampled
    if (not s.has_raw()):
        logger.info(
            'convert: %s: Raw file does not exist. No further action.', file_id)
        return None
    elif (s.has_resampled()):
        logger.info(
            'convert: %s: Resampled file exists. No further action.', file_id)
    else:
        s.convert()

    # diarize, check for seg
    if s.has_diarize():
        logger.info(
            'diarize: %s: Diarization file exists. No further action.', file_id)
    else:
        s.diarize()

    # seg_to_dict, check for temp
    if s.has_temp_seg_to_dict():
        logger.info('seg_to_dict: %s: Previously completed.', file_id)
    else:
        s.seg_to_dict()

    # split_resampled, check for temp
    if s.has_temp_dict_to_wav():
        logger.info('split_resampled: %s: Previously completed.', file_id)
    else:
        s.split_resampled()

    # recognize_diarize, check for temp
    if s.has_temp_wav_to_trans():
        logger.info('recognize_diarize: %s: Previously completed.', file_id)
    else:
        s.recognize_diarize()

    # write transcript
    s.write_transcript()

    return file_id


def workflow(method='diarize'):
    id_list = [file_id for file_id in os.listdir(
        data_dir) if os.path.isdir(os.path.join(data_dir, file_id))]
    if method not in ['diarize', 'sync', 'async']:
        logger.info('Invalid workflow method. Exiting.')
        return
    elif method == 'diarize':
        for file_id in id_list:
            try:
                diarize_pipeline(file_id)
            except:
                logger.error('diarize_pipeline: %s: Error occured.',
                             file_id, exc_info=1)
                continue
    elif method == 'sync':
        for file_id in id_list:
            try:
                sync_pipeline(file_id)
            except:
                logger.error('sync_pipeline: %s: Error occured.',
                             file_id, exc_info=1)
                continue
    else:
        for file_id in id_list:
            try:
                async_pipeline(file_id)
            except:
                logger.error('async_pipeline: %s: Error occured.',
                             file_id, exc_info=1)
                continue
    logger.info('Workflow completed.')

if __name__ == '__main__':
    if sys.argv[1] in ['-d', '--default', '--diarize']:
        workflow(method='diarize')
    elif sys.argv[1] in ['-s', '--sync']:
        workflow(method='sync')
    elif sys.argv[1] in ['-a', '--async']:
        workflow(method='async')
    else:
        logger.info('Invalid arguments. Exiting.')
