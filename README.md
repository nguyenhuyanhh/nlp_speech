# nlp_speech
This library performs speech file conversion using [SoX](http://sox.sourceforge.net), and speech recognition using Google Cloud Speech API.

## Requirements

A Linux system and Python 3. This library is developed for Python 3.5.2 on Lubuntu 16.04.1 LTS.

## Setup

1. Clone the project
1. Install SoX: `$ sudo apt-get install sox`
1. Install Python dependencies: `$ sudo pip3 install -r requirements.txt`
1. Run the main program: `$ python3 main.py`

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

1. If google-cloud supports Cloud Speech API, move code to that library instead of google-api-python-client
1. Diarization?
