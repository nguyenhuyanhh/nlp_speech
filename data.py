import shutil
import os
import logging

from slugify import slugify

AUDIO_EXTS = ['.wav', '.mp3']

# initialize path
cur_dir = os.path.dirname(os.path.realpath(__name__))
data_dir = os.path.join(cur_dir, 'data/')

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def flat_data(path):
    """
    Structure a flat folder into /data.
    Flat folder only contains speech files and no other subfolders.
    """
    for file in os.listdir(path):
        if (os.path.splitext(file)[1] in AUDIO_EXTS):
            file_path = os.path.join(path, file)
            file_id = slugify(file)
            working_dir = os.path.join(data_dir, file_id + '/')
            raw_dir = os.path.join(working_dir, 'raw/')
            resampled_dir = os.path.join(working_dir, 'resampled/')
            googleapi_dir = os.path.join(working_dir, 'googleapi/')
            diarize_dir = os.path.join(working_dir, 'diarization/')
            for dir in [raw_dir, resampled_dir, googleapi_dir, diarize_dir]:
                if not os.path.exists(dir):
                    os.makedirs(dir)
            shutil.copy2(file_path, raw_dir)
