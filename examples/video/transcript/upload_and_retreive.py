import os
from indexify import IndexifyClient

def summarize_debate(video_path):
    client = IndexifyClient()
    
    # Upload the video file
    content_id = client.upload_file("debate_summarizer", video_path)
    
    # Wait for the extraction to complete
    client.wait_for_extraction(content_id)
    
    # Retrieve the extracted topics
    topics = client.get_extracted_content(
        ingested_content_id=content_id,
        graph_name="debate_summarizer",
        policy_name="topic_extraction"
    )
    
    topics = topics[0]['content'].decode('utf-8')
    
    summaries = client.get_extracted_content(
        ingested_content_id=content_id,
        graph_name="debate_summarizer",
        policy_name="topic_summarization"
    )

    summaries = summaries[0]['content'].decode('utf-8')
    
    return topics, summaries

# Example usage
if __name__ == "__main__":
    video_path = "biden_trump_debate_2024.mp4"
    
    topics, summaries = summarize_debate(video_path)
    
    print("Debate Topics and Summaries:")
    print(topics, summaries)