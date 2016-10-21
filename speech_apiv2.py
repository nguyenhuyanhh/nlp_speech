import os

from google.cloud import speech
import sox

# init
cur_dir = os.path.dirname(os.path.realpath(__name__))
data_dir = os.path.join(cur_dir, 'data/')
client = speech.Client()


def convert(file):
    filename = os.path.splitext(file)[0]
    path_in = os.path.join(data_dir, file)
    path_out = os.path.join(data_dir, filename + '-resampled.wav')
    tfm = sox.Transformer()
    tfm.convert(samplerate=16000, n_channels=1, bitdepth=16)
    tfm.build(path_in, path_out)
    return path_out


def recognize_sync(file):
    path = os.path.join(data_dir, file)
    with open(path, 'rb') as f:
        content = f.read()
    sync_response = client.sync_recognize(
        content=content, encoding='LINEAR16', sample_rate=16000, language_code='en-US')
    return sync_response

print(recognize_sync('test-resampled.wav'))
