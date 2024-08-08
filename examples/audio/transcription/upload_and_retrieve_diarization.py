import requests

from indexify import IndexifyClient
import os

client = IndexifyClient()

def upload(file_url):
    print(f"downloading file {file_url}", flush=True)
    response = requests.get(file_url)
    response.raise_for_status()
    with open("audio.mp3", "wb") as f:
        f.write(response.content)
    content_id = client.upload_file("audio_diarization", "audio.mp3")
    return content_id

def retrieve(content_id):
    print(f"waiting for extraction of {content_id}", flush=True)
    client.wait_for_extraction(content_id)
    transcription = client.get_extracted_content(content_id, "audio_diarization", "speaker_diarization")
    print(f"diarized transcription: \n", flush=True)
    print(transcription)


if __name__ == "__main__":
    content_id = upload("https://pub-4df2354662e34feca92dcebb4ebc8590.r2.dev/The%20Last%20Thing%20To%20Ever%20Happen%20In%20The%20Universe.mp3")
    retrieve(content_id)
    os.remove("audio.mp3")