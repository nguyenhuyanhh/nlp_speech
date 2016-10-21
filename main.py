import speech_apiv1 as s

id_list = s.list_id()
for file_id in id_list:
    s.convert(file_id)
    s.upload(file_id)
    s.recognize_async(file_id)
