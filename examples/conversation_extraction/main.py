from pydantic import BaseModel, Field
from typing import List, Optional, Union
import logging
from indexify_extractor_sdk import Extractor, Content, Feature
from indexify.functions_sdk.graph import Graph
from indexify.functions_sdk.data_objects import File
from indexify.functions_sdk.image import Image
from indexify.functions_sdk.indexify_functions import indexify_function, indexify_router
import json
import io
import re
import datetime
from indexify import RemoteGraph
from dotenv import load_dotenv
import os
from llama_cpp import Llama

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class BaseConversationSchema(BaseModel):
    meeting_id: str
    date: datetime.datetime  
    duration: int
    participants: List[str]
    meeting_type: str
    summary: str

    class Config:
        arbitrary_types_allowed = True  

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

class StrategyMeetingSchema(BaseConversationSchema):
    key_decisions: List[str]
    risk_assessments: List[str]
    strategic_initiatives: List[str]
    action_items: List[str]

class SalesCallSchema(BaseConversationSchema):
    customer_pain_points: List[str]
    proposed_solutions: List[str]
    objections: List[str]
    next_steps: List[str]

class RDBrainstormSchema(BaseConversationSchema):
    innovative_ideas: List[str]
    technical_challenges: List[str]
    resource_requirements: List[str]
    potential_impacts: List[str]


base_image = (
    Image()
    .name("tensorlake/base-image")
)

transcribe_image = Image().name("tensorlake/transcriber").run("pip install faster_whisper")

llama_cpp_image = (
    Image()
    .name("tensorlake/llama-cpp")
    .run("apt-get update && apt-get install -y build-essential libgomp1")
    .run("pip install llama-cpp-python huggingface_hub")
    .run("apt-get purge -y build-essential && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*")
)

def get_chat_completion(prompt: str) -> str:
    model = Llama.from_pretrained(
        repo_id="NousResearch/Hermes-3-Llama-3.1-8B-GGUF",
        filename="*Q8_0.gguf", 
        verbose=True,
        n_ctx=60000,
    )
    output = model(prompt=prompt, max_tokens=30000, stop=["\n"])
    return output["choices"][0]["text"]

@indexify_function(image=transcribe_image)
def transcribe_audio(file: File) -> Transcription:
    """Transcribe audio and diarize speakers."""
    from faster_whisper import WhisperModel
    model = WhisperModel("base", device="cpu")
    segments, _ = model.transcribe(io.BytesIO(file.data))
    audio_segments = [SpeechSegment(text=segment.text, start_ts=segment.start, end_ts=segment.end) for segment in segments]
    return Transcription(segments=audio_segments)

@indexify_function(image=groq_image)  
def classify_meeting_intent(speech: Transcription) -> Transcription:
    try:
        """Classify the intent of the audio."""
        transcription_text = "\n".join([segment.text for segment in speech.segments])
        prompt = f"""
        Analyze the audio transcript and classify the intent of the audio FROM THE FOLLOWING OPTIONS ONLY:
        - strategy-meeting
        - sales-call
        - marketing-call
        - product-call
        - rd-brainstorm
        Required Output Format: intent: <intent>

        Transcription:
        {transcription_text}
        DO NOT ATTATCH ANY OTHER PHRASES,* symbols OR ANNOTATIONS WITH THE OUTPUT! ONLY the Intent in the required format.
        """
        response = get_chat_completion(prompt)
        intent = response.split(":")[-1].strip()  
        if intent in ["strategy-meeting", "sales-call", "marketing-call", "product-call", "rd-brainstorm"]:
            speech.classification = SpeechClassification(classification=intent, confidence=0.8)
        else:
            speech.classification = SpeechClassification(classification="unknown", confidence=0.5)
        
        return speech
    except Exception as e:
        logging.error(f"Error classifying meeting intent: {e}")
        speech.classification = SpeechClassification(classification="unknown", confidence=0.5)
        return speech

@indexify_function(image=groq_image)  
def summarize_strategy_meeting(speech: Transcription) -> StrategyMeetingSchema:
    """Summarize the strategy meeting."""
    transcription_text = "\n".join([segment.text for segment in speech.segments])
    prompt = f"""
    Analyze this strategy meeting transcript and extract data in this format only:
    1. Key decisions:\n
    2. Risk assessments:\n
    3. Strategic initiatives:\n
    4. Action items:\n

    Transcript:
    {transcription_text}
    DO NOT ATTATCH ANY OTHER PHRASES,* symbols OR ANNOTATIONS WITH THE OUTPUT! 
    """
    summary = get_chat_completion(prompt)
    lines = summary.split('\n')
    key_decisions = []
    risk_assessments = []
    strategic_initiatives = []
    action_items = []
    current_list = None
    for line in lines:
        line = line.strip() 
        if line.startswith('1. Key decisions:'):
            current_list = key_decisions
        elif line.startswith('2. Risk assessments:'):
            current_list = risk_assessments
        elif line.startswith('3. Strategic initiatives:'):
            current_list = strategic_initiatives
        elif line.startswith('4. Action items:'):
            current_list = action_items
        elif current_list is not None and line:  
            current_list.append(line.rstrip(', ')) 
    
    # Ensure all required fields are populated
    return StrategyMeetingSchema(
        meeting_id=f"Strategy-{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}",  
        date=datetime.datetime.now(),  
        duration=sum((segment.end_ts - segment.start_ts) for segment in speech.segments),  
        participants=[segment.speaker for segment in speech.segments if segment.speaker],  
        meeting_type="Strategy Meeting", 
        summary=summary,  
        key_decisions=key_decisions,  
        risk_assessments=risk_assessments, 
        strategic_initiatives=strategic_initiatives,
        action_items=action_items
    )

@indexify_function(image=groq_image) 
def summarize_sales_call(speech: Transcription) -> SalesCallSchema:
    """Summarize the sales call according to SalesCallSchema."""
    transcription_text = "\n".join([segment.text for segment in speech.segments])
    prompt = f"""
    Analyze this sales call transcript and extract data ONLY in this format only:
    1. Key details
    2. Client concerns
    3. Action items
    4. Next steps
    5. Recommendations for improving the approach

    Transcript:
    {transcription_text}
    DO NOT ATTATCH ANY OTHER PHRASES,* symbols OR ANNOTATIONS WITH THE OUTPUT!
    """
    summary = get_chat_completion(prompt)
    lines = summary.split('\n')
    customer_pain_points = []
    proposed_solutions = []
    objections = []
    next_steps = []
    current_list = None
    for line in lines:
        line = line.strip()
        if line.startswith('1. Customer Pain Points:'):
            current_list = customer_pain_points
        elif line.startswith('2. Proposed Solutions:'):
            current_list = proposed_solutions
        elif line.startswith('3. Objections:'):
            current_list = objections
        elif line.startswith('4. Next Steps:'):
            current_list = next_steps
        elif current_list is not None and line:
            current_list.append(line.rstrip(', '))

    # Ensure all required fields are populated
    return SalesCallSchema(
        meeting_id=f"RD-{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}",
        date=datetime.datetime.now(),
        duration=sum((segment.end_ts - segment.start_ts) for segment in speech.segments),
        participants=[segment.speaker for segment in speech.segments if segment.speaker],
        meeting_type="Sales Call",  
        summary=summary,  
        customer_pain_points=customer_pain_points,
        proposed_solutions=proposed_solutions,
        objections=objections,
        next_steps=next_steps
    )

@indexify_function(image=groq_image) 
def summarize_rd_brainstorm(speech: Transcription) -> RDBrainstormSchema:
    """Summarize the R&D brainstorming session according to RDBrainstormSchema."""
    transcription_text = "\n".join([segment.text for segment in speech.segments])
    prompt = f"""
    Analyze this sales call transcript and extract data ONLY in this format only:
    1. Innovative Ideas:
    2. Technical Challenges:
    3. Resource Requirements:
    4. Potential Impacts:

    Transcript:
    {transcription_text}
    DO NOT ATTATCH ANY OTHER PHRASES,* symbols OR ANNOTATIONS WITH THE OUTPUT!
    """
    summary = get_chat_completion(prompt)
    lines = summary.split('\n')
    innovative_ideas = []
    technical_challenges = []
    resource_requirements = []
    potential_impacts = []
    current_list = None
    for line in lines:
        line = line.strip()
        if line.startswith('1. Innovative Ideas:'):
            current_list = innovative_ideas
        elif line.startswith('2. Technical Challenges:'):
            current_list = technical_challenges
        elif line.startswith('3. Resource Requirements:'):
            current_list = resource_requirements
        elif line.startswith('4. Potential Impacts:'):
            current_list = potential_impacts
        elif current_list is not None and line:
            current_list.append(line.rstrip(', '))

    # Ensure all required fields are populated
    return RDBrainstormSchema(
        meeting_id=f"RD-{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}",
        date=datetime.datetime.now(),
        duration=sum((segment.end_ts - segment.start_ts) for segment in speech.segments),
        participants=[segment.speaker for segment in speech.segments if segment.speaker],
        meeting_type="R&D Brainstorm",
        summary=summary,
        innovative_ideas=innovative_ideas,
        technical_challenges=technical_challenges,
        resource_requirements=resource_requirements,
        potential_impacts=potential_impacts
    )

@indexify_router(image=base_image)
def router(speech: Transcription) -> List[Union[summarize_strategy_meeting, summarize_sales_call, summarize_rd_brainstorm]]:
    """Route the transcription to the appropriate summarizer based on the classification."""
    if speech.classification and speech.classification.classification == "strategy-meeting":
        return [summarize_strategy_meeting]
    elif speech.classification and speech.classification.classification in ["sales-call", "marketing-call", "product-call"]:
        return [summarize_sales_call]
    elif speech.classification and speech.classification.classification == "rd-brainstorm":
        return [summarize_rd_brainstorm]
    return []

def create_graph():
    g = Graph("Conversation_Extractor", start_node=transcribe_audio)
    g.add_edge(transcribe_audio, classify_meeting_intent)
    g.add_edge(classify_meeting_intent, router)
    g.route(router, [summarize_strategy_meeting, summarize_sales_call, summarize_rd_brainstorm])
    return g

def deploy_graphs(server_url: str):
    graph = create_graph()
    RemoteGraph.deploy(graph, server_url=server_url)
    logging.info("Graph deployed successfully")

def run_workflow(mode: str, server_url: str = 'http://localhost:8950', bucket_name: str = None, file_key: str = None):
    if mode == 'in-process-run':
        graph = create_graph()
    elif mode == 'remote-run':
        graph = Graph.by_name("Conversation_Extractor", server_url=server_url)
    else:
        raise ValueError("Invalid mode. Choose 'in-process-run' or 'remote-run'.")

    file_path = "data/audiofile"  #replace
    with open(file_path, 'rb') as audio_file:
        audio_data = audio_file.read()
    file = File(path=file_path, data=audio_data)

    invocation_id = graph.run(block_until_done=True, file=file)
    
    logging.info(f"Retrieving transcription for {invocation_id}")
    transcription = graph.output(invocation_id=invocation_id, fn_name=transcribe_audio.name)[0]
    for segment in transcription.segments:
        logging.info(f"{round(segment.start_ts, 2)} - {round(segment.end_ts, 2)}: {segment.text}")

    try:
        classification = graph.output(invocation_id=invocation_id, fn_name=classify_meeting_intent.name)[0].classification
        logging.info(f"Transcription Classification: {classification.classification}")

        if classification.classification == "strategy-meeting":
            summary = graph.output(invocation_id=invocation_id, fn_name=summarize_strategy_meeting.name)[0]
        elif classification.classification in ["sales-call", "marketing-call", "product-call"]:
            summary = graph.output(invocation_id=invocation_id, fn_name=summarize_sales_call.name)[0]
        elif classification.classification == "rd-brainstorm":
            summary = graph.output(invocation_id=invocation_id, fn_name=summarize_rd_brainstorm.name)[0]    
        else:
            logging.warning(f"No suitable summarization found for the classification: {classification.classification}")
            return

        logging.info(f"\nExtracted information:\n{summary.summary}")
    except Exception as e:
        logging.error(f"Error in workflow execution: {str(e)}")
        logging.error(f"Graph output for classify_meeting_intent: {graph.output(invocation_id=invocation_id, fn_name=classify_meeting_intent.name)}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run Conversation Extractor")
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












