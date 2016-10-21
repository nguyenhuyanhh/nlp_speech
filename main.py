import speech_apiv1 as s

# s.convert('test.wav')

# print(s.recognize_sync('test-resampled.wav'))

print(s.recognize_async('test-resampled.wav'))
