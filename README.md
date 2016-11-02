# nlp_speech
This library performs speech file conversion using [SoX](http://sox.sourceforge.net), speaker diarization using [LIUM](http://www-lium.univ-lemans.fr/diarization/doku.php/), and speech recognition using [Google Cloud Speech API](https://cloud.google.com/speech/).

## Requirements

1. Linux
1. Python (both 2.7 and 3.5 works)
1. Java (at least 1.6)

This library is developed using Python 2.7.12 on Lubuntu 16.04.1 LTS. The Java environment is 1.8.0_101.

## Setup

1. Clone the project
1. Install SoX with mp3 support: `$ sudo apt-get install sox libsox-fmt-all`
1. Install Python dependencies: `$ sudo pip install -r requirements.txt`
1. Run the main program: `$ python main.py`

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
            [file_id 1]-diarize.seg # lium output
            [diarized .wav files]
        transcript/
            [file_id 1]-sync.txt # transcript from Google Cloud Speech API (synchronous)
            [file_id 1]-async.txt # transcript from Google Cloud Speech API (asynchronous)
            [file_id 1]-diarize.txt # combined transcript from diarized files
            [file_id 1]-diarize.TextGrid # TextGrid file, to be passed to transcription editing software
            [file_id 1]-gold.txt # gold standard transcript
    [file_id 2]/
        ...
    ...
```

## To-dos

If google-cloud supports Cloud Speech API, move code to that library instead of google-api-python-client
