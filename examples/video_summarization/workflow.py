import io
from typing import List, Optional, Union
import logging

from pydantic import BaseModel, Field
from indexify import RemoteGraph
from indexify.functions_sdk.data_objects import File
from indexify.functions_sdk.graph import Graph
from indexify.functions_sdk.image import Image
from indexify.functions_sdk.indexify_functions import indexify_function, indexify_router

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Data Models
class YoutubeURL(BaseModel):
    url: str = Field(..., description="URL of the YouTube video")
    resolution: str = Field("480p", description="Resolution of the video")

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

class Summary(BaseModel):
    summary: str

# Image Definitions

base_image = (
    Image()
    .name("tensorlake/base-image")
)

yt_downloader_image = Image().name("tensorlake/yt-downloader").run("pip install pytubefix")
audio_image = (
    Image()
    .name("tensorlake/audio-processor")
    .run("apt-get update && apt-get install -y ffmpeg")
    .run("pip install pydub")
)
transcribe_image = Image().name("tensorlake/transcriber").run("pip install faster_whisper")
llama_cpp_image = (
    Image()
    .name("tensorlake/llama-cpp")
    .run("apt-get update && apt-get install -y build-essential libgomp1")
    .run("pip install llama-cpp-python huggingface_hub")
    .run("apt-get purge -y build-essential && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*")
)

# Indexify Functions
@indexify_function(image=yt_downloader_image)
def download_youtube_video(url: YoutubeURL) -> List[File]:
    """Download the YouTube video from the URL."""
    from pytubefix import YouTube
    yt = YouTube(url.url)
    logging.info("Downloading video...")
    content = yt.streams.first().download()
    logging.info("Video downloaded")
    return [File(data=content, mime_type="video/mp4")]

@indexify_function(image=audio_image)
def extract_audio_from_video(file: File) -> File:
    """Extract the audio from the video."""
    from pydub import AudioSegment
    audio = AudioSegment.from_file(file.data)
    return File(data=audio.export("audio.wav", format="wav").read(), mime_type="audio/wav")

@indexify_function(image=transcribe_image)
def transcribe_audio(file: File) -> Transcription:
    """Transcribe audio and diarize speakers."""
    from faster_whisper import WhisperModel
    model = WhisperModel("base", device="cpu")
    segments, _ = model.transcribe(io.BytesIO(file.data))
    audio_segments = [SpeechSegment(text=segment.text, start_ts=segment.start, end_ts=segment.end) for segment in segments]
    return Transcription(segments=audio_segments)

@indexify_function(image=llama_cpp_image)
def classify_meeting_intent(speech: Transcription) -> Transcription:
    try:
        """Classify the intent of the audio."""
        from llama_cpp import Llama
        model = Llama.from_pretrained(
            repo_id="NousResearch/Hermes-3-Llama-3.1-8B-GGUF",
            filename="*Q8_0.gguf",
            verbose=True,
            n_ctx=60000,
        )
        transcription_text = "\n".join([segment.text for segment in speech.segments])
        prompt = f"""
        Analyze the audio transcript and classify the intent of the audio FROM THE FOLLOWING OPTIONS ONLY:
        - job-interview
        - sales-call
        - customer-support-call
        - technical-support-call
        - marketing-call
        - product-call
        - financial-call
        Required Output Format: intent: <intent>

        Transcription:
        {transcription_text}
        DO NOT ATTATCH ANY OTHER PHRASES,* symbols OR ANNOTATIONS WITH THE OUTPUT! Provide ONLY the intent in the required format.
        """
        output = model(prompt=prompt, max_tokens=50, stop=["\n"])
        response = output["choices"][0]["text"]
        print(f"response: {response}")
        intent = response.split(":")[-1].strip()
        if intent in ["job-interview", "sales-call", "customer-support-call", "technical-support-call", "marketing-call", "product-call", "financial-call"]:
            speech.classification = SpeechClassification(classification=intent, confidence=0.8)
        else:
            speech.classification = SpeechClassification(classification="unknown", confidence=0.5)
        return speech
    except Exception as e:
        logging.error(f"Error classifying meeting intent: {e}")
        speech.classification = SpeechClassification(classification="unknown", confidence=0.5)
        return speech

@indexify_function(image=llama_cpp_image)
def summarize_job_interview(speech: Transcription) -> Summary:
    """Summarize the job interview."""
    from llama_cpp import Llama
    model = Llama.from_pretrained(
        repo_id="NousResearch/Hermes-3-Llama-3.1-8B-GGUF",
        filename="*Q8_0.gguf",
        verbose=True,
        n_ctx=60000,
    )
    transcription_text = "\n".join([segment.text for segment in speech.segments])
    prompt = f"""
    Analyze this job interview transcript and summarize the key points in the below format ONLY:
    1. Candidate's Strengths and Qualifications
    2. Key Responses and Insights
    3. Cultural Fit and Soft Skills
    4. Areas of Concern or Improvement
    5. Overall Impression and Recommendation

    Transcript:
    {transcription_text[:1000]}
    DO NOT ATTATCH ANY OTHER PHRASES,* symbols OR ANNOTATIONS WITH THE OUTPUT! 
    """
    output = model(prompt=prompt, max_tokens=30000, stop=["\n"])
    return Summary(summary=output["choices"][0]["text"])

@indexify_function(image=llama_cpp_image)
def summarize_sales_call(speech: Transcription) -> Summary:
    """Summarize the sales call."""
    from llama_cpp import Llama
    model = Llama.from_pretrained(
        repo_id="NousResearch/Hermes-3-Llama-3.1-8B-GGUF",
        filename="*Q8_0.gguf",
        verbose=True,
        n_ctx=60000,
    )
    transcription_text = "\n".join([segment.text for segment in speech.segments])
    prompt = f"""
    Analyze this sales call transcript and summarize in the below format ONLY:
    1. Key details
    2. Client concerns
    3. Action items
    4. Next steps
    5. Recommendations for improving the approach

    Transcript:
    {transcription_text}
    DO NOT ATTATCH ANY OTHER PHRASES,* symbols OR ANNOTATIONS WITH THE OUTPUT!
    """
    output = model(prompt=prompt, max_tokens=30000, stop=["\n"])
    return Summary(summary=output["choices"][0]["text"])

@indexify_router(image=base_image)
def route_transcription_to_summarizer(speech: Transcription) -> List[Union[summarize_job_interview, summarize_sales_call]]:
    """Route the transcription to the appropriate summarizer based on the classification."""
    if speech.classification.classification == "job-interview":
        return [summarize_job_interview]
    elif speech.classification.classification in ["sales-call", "marketing-call", "product-call"]:
        return [summarize_sales_call]
    return []

def create_graph():
    g = Graph("Youtube_Video_Summarizer", start_node=download_youtube_video)
    g.add_edge(download_youtube_video, extract_audio_from_video)
    g.add_edge(extract_audio_from_video, transcribe_audio)
    g.add_edge(transcribe_audio, classify_meeting_intent)
    g.add_edge(classify_meeting_intent, route_transcription_to_summarizer)
    g.route(route_transcription_to_summarizer, [summarize_job_interview, summarize_sales_call])
    return g

def deploy_graphs(server_url: str):
    graph = create_graph()
    RemoteGraph.deploy(graph, server_url=server_url)
    logging.info("Graph deployed successfully")

def run_workflow(mode: str, server_url: str = 'http://localhost:8900'):
    if mode == 'in-process-run':
        graph = create_graph()
    elif mode == 'remote-run':
        graph = RemoteGraph.by_name("Youtube_Video_Summarizer", server_url=server_url)
    else:
        raise ValueError("Invalid mode. Choose 'in-process-run' or 'remote-run'.")

    youtube_url = "https://www.youtube.com/watch?v=gjHv4pM8WEQ"
    invocation_id = graph.run(block_until_done=True, url=YoutubeURL(url=youtube_url))
    
    logging.info(f"Retrieving transcription for {invocation_id}")
    transcription = graph.output(invocation_id=invocation_id, fn_name=transcribe_audio.name)[0]
    for segment in transcription.segments:
        logging.info(f"{round(segment.start_ts, 2)} - {round(segment.end_ts, 2)}: {segment.text}")

    try:
        classification = graph.output(invocation_id=invocation_id, fn_name=classify_meeting_intent.name)[0].classification
        logging.info(f"Transcription Classification: {classification.classification}")

        if classification.classification == "job-interview":
            summary = graph.output(invocation_id=invocation_id, fn_name=summarize_job_interview.name)[0]
        elif classification.classification in ["sales-call", "marketing-call", "product-call"]:
            summary = graph.output(invocation_id=invocation_id, fn_name=summarize_sales_call.name)[0]
        else:
            logging.warning(f"No suitable summarization found for the classification: {classification.classification}")
            return

        logging.info(summary.summary)
    except Exception as e:
        logging.error(f"Error in workflow execution: {str(e)}")
        logging.error(f"Graph output for classify_meeting_intent: {graph.output(invocation_id=invocation_id, fn_name=classify_meeting_intent.name)}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run YouTube Video Summarizer")
    parser.add_argument('--mode', choices=['in-process-run', 'remote-deploy', 'remote-run'], required=True, 
                        help='Mode of operation: in-process-run, remote-deploy, or remote-run')
    parser.add_argument('--server-url', default='http://localhost:8900', help='Indexify server URL for remote mode or deployment')
    args = parser.parse_args()

    try:
        if args.mode == 'remote-deploy':
            deploy_graphs(args.server_url)
        elif args.mode in ['in-process-run', 'remote-run']:
            run_workflow(args.mode, args.server_url)
        logging.info("Operation completed successfully!")
    except Exception as e:
        logging.error(f"An error occurred during execution: {str(e)}")
