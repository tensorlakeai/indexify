import io
from typing import List, Optional, Union

from pydantic import BaseModel, Field
from rich import print

from indexify import RemoteGraph
from indexify.functions_sdk.data_objects import File
from indexify.functions_sdk.graph import Graph
from indexify.functions_sdk.image import Image
from indexify.functions_sdk.indexify_functions import (
    indexify_function,
    indexify_router,
)


class YoutubeURL(BaseModel):
    url: str = Field(..., description="URL of the youtube video")
    resolution: str = Field("480p", description="Resolution of the video")


yt_downloader_image = (
    Image().name("yt-image-1").run("pip install pytubefix")
)


@indexify_function(image=yt_downloader_image)
def download_youtube_video(url: YoutubeURL) -> List[File]:
    """
    Download the youtube video from the url.
    """
    from pytubefix import YouTube

    yt = YouTube(url.url)
    # content = yt.streams.filter(res=url.resolution).first().download()
    # This doesn't always work as YT might not have the resolution specified
    print("Downloading video...")
    content = yt.streams.first().download()
    print("Video downloaded")
    return [File(data=content, mime_type="video/mp4")]


audio_image = (
    Image().name("audio-image-1").run("pip install pydub")
)


@indexify_function(image=audio_image)
def extract_audio_from_video(file: File) -> File:
    """
    Extract the audio from the video.
    """
    from pydub import AudioSegment

    audio = AudioSegment.from_file(file.data)
    return File(
        data=audio.export("audio.wav", format="wav").read(), mime_type="audio/wav"
    )


class SpeechSegment(BaseModel):
    speaker: Optional[str] = None
    text: str
    start_ts: float
    end_ts: float


class SpeechClassification(BaseModel):
    classification: str
    confidence: float


class Transcription(BaseModel):
    segments: List[SpeechSegment]
    classification: Optional[SpeechClassification] = None


transcribe_image = (
    Image()
    .name("transcribe-image-1")
    .run("pip install faster_whisper")
)


@indexify_function(image=transcribe_image)
def transcribe_audio(file: File) -> Transcription:
    """
    Transcribe audio and diarize speakers.
    """
    from faster_whisper import WhisperModel

    model = WhisperModel("base", device="cpu")
    segments, _ = model.transcribe(io.BytesIO(file.data))
    audio_segments = []
    for segment in segments:
        audio_segments.append(
            SpeechSegment(text=segment.text, start_ts=segment.start, end_ts=segment.end)
        )
    return Transcription(segments=audio_segments)


llama_cpp_image = (
    Image()
    .name("classify-image-1")
    .run("apt-get update && apt-get install -y build-essential")
    .run("pip install llama-cpp-python")
    .run(
        "apt-get purge -y build-essential && "
        "apt-get autoremove -y && rm -rf /var/lib/apt/lists/*"
    )
)


@indexify_function(image=llama_cpp_image)
def classify_meeting_intent(speech: Transcription) -> Transcription:
    """
    Classify the intent of the audio.
    """
    from llama_cpp import Llama

    model = Llama.from_pretrained(
        repo_id="NousResearch/Hermes-3-Llama-3.1-8B-GGUF",
        filename="*Q8_0.gguf",
        verbose=True,
        n_ctx=60000,
    )
    transcription_text = "\n".join([segment.text for segment in speech.segments])
    prompt = f"""
    You are a helpful assistant that classifies the intent of the audio.
    Classify the intent of the audio. These are the possible intents:
    - job-interview
    - sales-call
    - customer-support-call
    - technical-support-call
    - marketing-call
    - product-call
    - financial-call
    Write the intent of the audio in the following format:
    intent: <intent>

    The transcription of the audio is:
    {transcription_text}
    """
    output = model(prompt=prompt, max_tokens=50, stop=["\n"])
    response = output["choices"][0]["text"]
    print(f"response: {response}")
    output_tokens = response.split(":")
    if len(output_tokens) > 1:
        if output_tokens[0].strip() == "intent":
            if output_tokens[1].strip() in [
                "job-interview",
                "sales-call",
                "customer-support-call",
                "technical-support-call",
                "marketing-call",
                "product-call",
                "financial-call",
            ]:
                speech.classification = SpeechClassification(
                    classification=output_tokens[1].strip(), confidence=0
                )
                return speech
    speech.classification = SpeechClassification(classification="unknown", confidence=0)
    return speech


class Summary(BaseModel):
    summary: str


@indexify_function(image=llama_cpp_image)
def summarize_job_interview(speech: Transcription) -> Summary:
    """
    Summarize the job interview.
    """
    from llama_cpp import Llama

    model = Llama.from_pretrained(
        repo_id="NousResearch/Hermes-3-Llama-3.1-8B-GGUF",
        filename="*Q8_0.gguf",
        verbose=True,
        n_ctx=60000,
    )
    transcription_text = "\n".join([segment.text for segment in speech.segments])
    prompt = f"""
    I have a transcript of a job interview that took place on [date]. The interview included discussions about the candidate’s background, skills, and experience, as well as their
    responses to specific questions and scenarios. Please summarize the key points from the interview, including:

    1. Candidate’s Strengths and Qualifications: Highlight any notable skills, experiences, or achievements mentioned.
    2. Key Responses and Insights: Summarize the candidate’s answers to important questions or scenarios.
    3. Cultural Fit and Soft Skills: Provide an overview of the candidate’s fit with the company culture and any relevant soft skills.
    4. Areas of Concern or Improvement: Note any reservations or areas where the candidate might need further development.
    5. Overall Impression and Recommendation: Offer a brief assessment of the candidate’s suitability for the role and any suggested next steps.

    The transcript is:
    {transcription_text}
    """
    output = model(prompt=prompt, max_tokens=30000, stop=["\n"])
    response = output["choices"][0]["text"]
    return Summary(summary=response)


@indexify_function(image=llama_cpp_image)
def summarize_sales_call(speech: Transcription) -> Summary:
    """
    Summarize the sales call.
    """
    from llama_cpp import Llama

    model = Llama.from_pretrained(
        repo_id="NousResearch/Hermes-3-Llama-3.1-8B-GGUF",
        filename="*Q8_0.gguf",
        verbose=True,
        n_ctx=60000,
    )
    transcription_text = "\n".join([segment.text for segment in speech.segments])
    prompt = f"""
    I had a sales call with a prospective client earlier today. The main points of the conversation included [briefly describe key topics discussed, such as client needs, product features,
    objections, and any agreements or follow-ups]. Please summarize the call, highlighting the key details, client concerns, and any action items or next steps. Additionally,
    if there are any recommendations for improving our approach based on the discussion, please include those as well

    The transcript is:
    {transcription_text}
    """
    output = model(prompt=prompt, max_tokens=30000, stop=["\n"])
    response = output["choices"][0]["text"]
    return Summary(summary=response)


@indexify_router()
def route_transcription_to_summarizer(
    speech: Transcription,
) -> List[Union[summarize_job_interview, summarize_sales_call]]:
    """
    Route the transcription to the summarizer based on the classification result from
    the classify_text_feature extractor.
    """
    if speech.classification.classification == "job-interview":
        return summarize_job_interview
    elif speech.classification.classification in [
        "sales-call",
        "marketing-call",
        "product-call",
    ]:
        return summarize_sales_call
    return None


def create_graph():
    g = Graph("Youtube_Video_Summarizer", start_node=download_youtube_video)
    g.add_edge(download_youtube_video, extract_audio_from_video)
    g.add_edge(extract_audio_from_video, transcribe_audio)
    g.add_edge(transcribe_audio, classify_meeting_intent)
    g.add_edge(classify_meeting_intent, route_transcription_to_summarizer)

    g.route(
        route_transcription_to_summarizer,
        [summarize_job_interview, summarize_sales_call],
    )
    return g


if __name__ == "__main__":
    g = create_graph()
    remote_graph = g
    #remote_graph = RemoteGraph.deploy(g)
    invocation_id = remote_graph.run(
        block_until_done=True,
        url=YoutubeURL(url="https://www.youtube.com/watch?v=gjHv4pM8WEQ"),
    )
    print(f"[bold] Retrieving transcription for {invocation_id} [/bold]")
    outputs = remote_graph.output(
        invocation_id=invocation_id,
        fn_name=transcribe_audio.name,
    )
    transcription = outputs[0]
    for segment in transcription.segments:
        print(
            f"[bold] {round(segment.start_ts, 2)} - {round(segment.end_ts, 2)} [/bold]: {segment.text}"
        )

    transcription_with_classification = remote_graph.output(
        invocation_id=invocation_id,
        fn_name=classify_meeting_intent.name,
    )

    if len(transcription_with_classification) == 0:
        print("[bold] No classification found [/bold]")
        exit(1)

    classification = transcription_with_classification[0].classification

    print(
        f"[bold] Transcription Classification: {classification.classification} [/bold]"
    )

    print(f"[bold] Summarization of transcription [/bold]")
    if classification.classification == "job-interview":
        summary = remote_graph.output(
            invocation_id=invocation_id,
            fn_name=summarize_job_interview.name,
        )
        print(summary[0].summary)
    elif classification.classification in [
        "sales-call",
        "marketing-call",
        "product-call",
    ]:
        summary = remote_graph.output(
            invocation_id=invocation_id,
            fn_name=summarize_sales_call.name,
        )
        print(summary[0].summary)
