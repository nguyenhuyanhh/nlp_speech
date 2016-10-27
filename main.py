import logging

import data
import speech

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# data.flat_data('/home/nhanh/test')
# speech.sync_workflow()

s = speech.Speech('leon-perera-test-wav')
s.diarize()
# speech.async_pipeline('leon-perera-test-wav')
