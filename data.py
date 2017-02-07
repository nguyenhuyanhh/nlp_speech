"""Data operations."""

import json
import logging
import os
import shutil
import sys
import wave
from decimal import Decimal

from slugify import slugify

AUDIO_EXTS = ['.wav', '.mp3']

# initialize path and logger
CUR_DIR = os.path.dirname(os.path.realpath(__name__))
DATA_DIR = os.path.join(CUR_DIR, 'data/')
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)
logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)


def crawl_folder(path, ext_list):
    """
    Crawl a folder for specific types of file.
    Copy all such files into /data_crawl.
    """
    crawl_dir = os.path.join(CUR_DIR, 'data_crawl/')
    if not os.path.exists(crawl_dir):
        os.makedirs(crawl_dir)
    for root, _, files in os.walk(path):
        for file_ in files:
            if os.path.splitext(file_)[1] in ext_list:
                shutil.copy2(os.path.join(root, file_), crawl_dir)
                LOG.info('Processed %s', file_)


def import_folder(path):
    """
    Import a flat folder into /data.
    Flat folder only contains speech files and no other subfolders.
    """
    for file_ in os.listdir(path):
        if os.path.splitext(file_)[1] in AUDIO_EXTS:
            file_path = os.path.join(path, file_)
            file_id = slugify(os.path.splitext(file_)[0])
            working_dir = os.path.join(DATA_DIR, file_id + '/')
            raw_dir = os.path.join(working_dir, 'raw/')
            resampled_dir = os.path.join(working_dir, 'resampled/')
            diarize_dir = os.path.join(working_dir, 'diarization/')
            trans_dir = os.path.join(working_dir, 'transcript/')
            googleapi_dir = os.path.join(trans_dir, 'googleapi/')
            textgrid_dir = os.path.join(trans_dir, 'textgrid/')
            dir_list = [raw_dir, resampled_dir,
                        diarize_dir, trans_dir, googleapi_dir, textgrid_dir]
            for dir_ in dir_list:
                if not os.path.exists(dir_):
                    os.makedirs(dir_)
            shutil.copy2(file_path, raw_dir)
            LOG.info('Processed %s', file_id)


def clear_temp(path):
    """
    Clear all /temp folders from a path.
    Useful to prepare completed folder for long term storage.
    """
    # get list of folders that conforms to /data structure
    key = set(['raw', 'resampled', 'diarization', 'transcript', 'temp'])
    completed_dirs = set()
    for root, dirs, _ in os.walk(path):
        if key.issubset(dirs):
            completed_dirs.add(root)

    for dir_ in completed_dirs:
        temp_dir = os.path.join(dir_, 'temp/')
        shutil.rmtree(temp_dir, ignore_errors=True)
        LOG.info('Processed %s', dir_)


def migrate(path):
    """
    Convert all folders in a path from old to new structure.
    Path must contain all folders with the old structure.
    """
    for dir_ in os.listdir(path):
        working_dir = os.path.join(path, dir_)
        resampled_dir = os.path.join(working_dir, 'resampled/')
        diarize_dir = os.path.join(working_dir, 'diarization/')
        trans_dir = os.path.join(working_dir, 'transcript/')
        googleapi_dir = os.path.join(trans_dir, 'googleapi/')
        textgrid_dir = os.path.join(trans_dir, 'textgrid/')

        old_resampled = os.path.join(resampled_dir, dir_ + '-resampled.wav')
        new_resampled = os.path.join(resampled_dir, dir_ + '.wav')
        old_diarize = os.path.join(diarize_dir, dir_ + '-diarize.seg')
        new_diarize = os.path.join(diarize_dir, dir_ + '.seg')
        old_trans = os.path.join(googleapi_dir, dir_ + '-diarize.txt')
        new_trans = os.path.join(googleapi_dir, dir_ + '.txt')
        old_textgrid = os.path.join(textgrid_dir, dir_ + '-diarize.TextGrid')
        new_textgrid = os.path.join(textgrid_dir, dir_ + '.TextGrid')

        if os.path.exists(old_resampled):
            os.rename(old_resampled, new_resampled)
        if os.path.exists(old_diarize):
            os.rename(old_diarize, new_diarize)
        if os.path.exists(old_trans):
            os.rename(old_trans, new_trans)
        if os.path.exists(old_textgrid):
            os.rename(old_textgrid, new_textgrid)

        LOG.info('Processed %s', dir_)


def stats(path):
    """
    Return some statistics for the folder.
    Useful for completion statistics.
    """
    # initialize stats.json
    stats_file = os.path.join(CUR_DIR, 'stats.json')
    if not os.path.exists(stats_file):
        stats = dict()
    else:
        with open(stats_file, 'r') as file_:
            stats = json.load(file_)

    # get list of folders that conforms to /data structure
    key = set(['raw', 'resampled', 'diarization', 'transcript'])
    completed_dirs = dict()
    for root, dirs, _ in os.walk(path):
        if key.issubset(dirs):
            file_id = os.path.basename(os.path.normpath(root))
            completed_dirs[file_id] = root

    # update if id not in stats.json
    for file_id, root in completed_dirs.items():
        resampled_dir = os.path.join(root, 'resampled/')
        if file_id not in stats.keys():
            if len(os.listdir(resampled_dir)) == 1:
                resampled_file = os.path.join(
                    resampled_dir, os.listdir(resampled_dir)[0])
                file_ = wave.open(resampled_file, 'r')
                duration = Decimal(file_.getnframes()) / file_.getframerate()
                file_.close()
                stats[file_id] = str(duration)
                LOG.info('Updated %s', file_id)

    # calculate and convert to human readable times
    time = Decimal(0)
    for id_, dur_ in stats.items():
        if id_ in completed_dirs.keys():
            time += Decimal(dur_)
    hours = int(time / 3600)
    time -= hours * 3600
    minutes = int(time / 60)
    seconds = time - minutes * 60

    # update stats.json
    with open(stats_file, 'w') as file_out:
        json.dump(stats, file_out, sort_keys=True, indent=4)

    LOG.info('Processed %s files, total time %s hours %s minutes %s seconds.',
             len(stats), hours, minutes, seconds)


def print_completed(path):
    """
    Print a list of completed file_ids in a path.
    Useful to check /data.
    """
    # get list of folders that conforms to /data structure
    key = set(['raw', 'resampled', 'diarization', 'transcript'])
    completed_dirs = set()
    for root, dirs, _ in os.walk(path):
        if key.issubset(dirs):
            completed_dirs.add(root)

    count = 0
    for dir_ in sorted(completed_dirs):
        textgrid_dir = os.path.join(dir_, 'transcript/textgrid')
        if len(os.listdir(textgrid_dir)) == 1:
            print dir_
            count += 1

    LOG.info('%s file_ids completed.', count)

if __name__ == '__main__':
    if sys.argv[1] in ['-r', '--crawl']:
        if len(sys.argv) <= 3:
            LOG.info('Invalid arguments.')
        else:
            crawl_folder(sys.argv[2], sys.argv[3:])
    elif (sys.argv[1] in ['-i', '--import']):
        import_folder(sys.argv[2])
    elif (sys.argv[1] in ['-c', '--clear']):
        clear_temp(sys.argv[2])
    elif (sys.argv[1] in ['-m', '--migrate']):
        migrate(sys.argv[2])
    elif (sys.argv[1] in ['-s', '--stats']):
        stats(sys.argv[2])
    elif (sys.argv[1] in ['-p', '--print-completed']):
        print_completed(sys.argv[2])
    else:
        LOG.info('Invalid arguments.')
