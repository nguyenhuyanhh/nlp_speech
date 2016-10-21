# nlp_speech
This library performs speech file conversion and speech recognition using Google Cloud Speech API.

## Requirements

A Linux system and Python (both 2.7 and 3.5 works). This library is developed for Python 2.7.12 on Lubuntu 16.04.1 LTS.

## Setup

1. Clone the project
1. Install Python dependencies: `$ sudo pip install -r requirements.txt`
1. Load raw data into `/data`
1. Run the main program: `$ python ./main.py`

## Data folder structure

The structure of `/data` is as follows:

```
data/
    [file_id 1]/
        raw/
            [raw_file] # can be in any format, must provide
        resampled/
            [file_id 1]-resampled.wav
        diarization/
            [diarization_files]
        googleapi/
            [file_id 1]-async.txt # transcript from Google Cloud Speech API
            [file_id 1]-async.txt.gold # gold standard transcript
    [file_id 2]/
        ...
    ...
```

## To-dos

1. Implement multi-processing
1. If API library v2 is available, move code to that library instead for cleaner code (see `speech_apiv2.py` for sample)
1. Diarization?
