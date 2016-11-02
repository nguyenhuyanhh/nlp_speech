import shutil
import os
import logging

from slugify import slugify

AUDIO_EXTS = ['.wav', '.mp3']

# initialize path and logger
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
            diarize_dir = os.path.join(working_dir, 'diarization/')
            trans_dir = os.path.join(working_dir, 'transcript/')
            googleapi_dir = os.path.join(trans_dir, 'googleapi/')
            textgrid_dir = os.path.join(trans_dir, 'textgrid/')
            for dir in [raw_dir, resampled_dir, diarize_dir, trans_dir, googleapi_dir, textgrid_dir]:
                if not os.path.exists(dir):
                    os.makedirs(dir)
            shutil.copy2(file_path, raw_dir)
            logger.info('Processed %s', file_id)


def clear_intermediate(path):
    """
    Clear all intermediate files (resampled and diarization) from a path.
    Path must contain all folders with the structure described in /README.md.
    """
    for dir in os.listdir(path):
        working_dir = os.path.join(path, dir)
        resampled_dir = os.path.join(working_dir, 'resampled/')
        diarize_dir = os.path.join(working_dir, 'diarization/')
        shutil.rmtree(resampled_dir, ignore_errors=True)
        shutil.rmtree(diarize_dir, ignore_errors=True)
        logger.info('Processed %s', dir)


def clear_intermediate_old(path):
    """
    Clear all intermediate files (resampled and diarization) from a path.
    Path must contain all folders with the old structure
    """
    for dir in os.listdir(path):
        working_dir = os.path.join(path, dir)
        resampled_dir = os.path.join(working_dir, 'resampled/')
        diarize_dir = os.path.join(working_dir, 'diarization/')
        old_googleapi_dir = os.path.join(working_dir, 'googleapi/')
        googleapi_dir = os.path.join(working_dir, 'transcript/googleapi/')
        textgrid_dir = os.path.join(working_dir, 'transcript/textgrid/')
        os.makedirs(googleapi_dir)
        os.makedirs(textgrid_dir)
        old_trans = os.path.join(diarize_dir, dir + '-diarize.txt')
        new_trans = os.path.join(googleapi_dir, dir + '-diarize.txt')
        shutil.move(old_trans, new_trans)
        old_textgrid = os.path.join(diarize_dir, dir + '-diarize.TextGrid')
        new_textgrid = os.path.join(textgrid_dir, dir + '-diarize.TextGrid')
        shutil.move(old_textgrid, new_textgrid)
        shutil.rmtree(resampled_dir, ignore_errors=True)
        shutil.rmtree(diarize_dir, ignore_errors=True)
        shutil.rmtree(old_googleapi_dir, ignore_errors=True)
        logger.info('Processed %s', dir)
