import logging

import data
import speech

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# data.flat_data('/home/nhanh/test')
# speech.sync_workflow()
# speech.async_pipeline('on-the-record-leon-perera-mp3')
speech.diarize_pipeline('on-the-record-leon-perera-mp3')
# speech.async_workflow()

# s = speech.Speech('leon-perera-test-wav')
# s.diarize()
