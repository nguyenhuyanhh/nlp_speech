import speech

id_list = speech.list_id()
for file_id in id_list:
    speech.async_pipeline(file_id)
