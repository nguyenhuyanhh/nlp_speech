import shutil
import os
import logging
import sys
from decimal import Decimal

from slugify import slugify

AUDIO_EXTS = ['.wav', '.mp3']

# initialize path and logger
cur_dir = os.path.dirname(os.path.realpath(__name__))
data_dir = os.path.join(cur_dir, 'data/')
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def import_folder(path):
    """
    Import a flat folder into /data.
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


def migrate(path):
    """
    Convert all folders in a path from old to new structure.
    Path must contain all folders with the old structure.
    """
    for dir in os.listdir(path):
        working_dir = os.path.join(path, dir)
        resampled_dir = os.path.join(working_dir, 'resampled/')
        diarize_dir = os.path.join(working_dir, 'diarization/')
        old_googleapi_dir = os.path.join(working_dir, 'googleapi/')
        trans_dir = os.path.join(working_dir, 'transcript/')
        new_googleapi_dir = os.path.join(trans_dir, 'googleapi/')
        textgrid_dir = os.path.join(trans_dir, 'textgrid/')
        os.makedirs(textgrid_dir)
        shutil.move(old_googleapi_dir, trans_dir)
        old_trans = os.path.join(diarize_dir, dir + '-diarize.txt')
        new_trans = os.path.join(new_googleapi_dir, dir + '-diarize.txt')
        shutil.move(old_trans, new_trans)
        old_textgrid = os.path.join(diarize_dir, dir + '-diarize.TextGrid')
        new_textgrid = os.path.join(textgrid_dir, dir + '-diarize.TextGrid')
        shutil.move(old_textgrid, new_textgrid)
        logger.info('Processed %s', dir)


def stats(path):
    """
    Return some statistics for the folder.
    Useful for completion statistics.
    """
    # get list of folders that conforms to /data structure
    key = set(['raw', 'resampled', 'diarization', 'transcript'])
    completed_dirs = set()
    for root, dirs, files in os.walk(path):
        if key.issubset(dirs):
            completed_dirs.add(root)

    # get total time and no of file_ids processed
    count = 0
    time = Decimal(0.0)
    for dir in completed_dirs:
        resampled_dir = os.path.join(dir, 'resampled/')
        if (len(os.listdir(resampled_dir)) == 1):
            resampled_file = os.path.join(
                resampled_dir, os.listdir(resampled_dir)[0])
            time += Decimal(os.path.getsize(resampled_file)) / 32000
            count += 1

    # convert to human readable times
    hours = int(time / 3600)
    time -= hours * 3600
    minutes = int(time / 60)
    seconds = time - minutes * 60

    logger.info('Processed %s files, total time %s hours %s minutes %s seconds.',
                count, hours, minutes, seconds)

if __name__ == '__main__':
    if len(sys.argv) != 3:
        logger.info('Invalid arguments. Exiting.')
    elif (sys.argv[1] in ['-i', '--import']):
        import_folder(sys.argv[2])
    elif (sys.argv[1] in ['-c', '--clear']):
        clear_intermediate(sys.argv[2])
    elif (sys.argv[1] in ['-m', '--migrate']):
        migrate(sys.argv[2])
    elif (sys.argv[1] in ['-s', '--stats']):
        stats(sys.argv[2])
    else:
        logger.info('Invalid arguments. Exiting.')
