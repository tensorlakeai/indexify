# Debate Topic-wise Summary Pipeline with Indexify and Mistral

We show how to create a pipeline capable of summarizing and performing topic extraction on videos.
The pipeline will consist of four main steps -

1. Video to Audio extraction using `tensorlake/audio-extractor`
2. Speech recognition and diarization using `tensorlake/asrdiarization`
3. Topic extraction using `tensorlake/mistral`
4. Topic-wise summarization using `tensorlake/mistral`

## Prerequisites

Before we begin, ensure you have the following:

- Create a virtual env with Python 3.9 or later
  ```shell
  python3.9 -m venv ve
  source ve/bin/activate
  ```
- `pip` (Python package manager)
- A Mistral API key
- Basic familiarity with Python and command-line interfaces

## Setup

### Install Indexify

First, let's install Indexify using the official installation script:

```bash
curl https://getindexify.ai | sh
```

Start the Indexify server:
```bash
./indexify server -d
```

### Install Required Extractors

Next, we'll install the necessary extractors in a new terminal.

```bash
pip install indexify-extractor-sdk
indexify-extractor download tensorlake/audio-extractor
indexify-extractor download tensorlake/asrdiarization
indexify-extractor download tensorlake/mistral
indexify-extractor download tensorlake/openai
```

Once the extractors are downloaded, start them:
```bash
indexify-extractor join-server
```

## Creating the Extraction Graph

Now we will setup the extraction graph for video ingestion, and we will also write a script to query the topics and transcription.

On a new terminal, install the Indexify client library
```bash
pip install -U indexify pytubefix
```

Create a new Python file called `debate_summary_graph.py` and add the following code:

```python
from indexify import IndexifyClient, ExtractionGraph

client = IndexifyClient()

extraction_graph_spec = """
name: 'debate_summarizer'
extraction_policies:
  - extractor: 'tensorlake/audio-extractor'
    name: 'video_to_audio'
  - extractor: 'tensorlake/asrdiarization'
    name: 'speech_recognition'
    content_source: 'video_to_audio'
  - extractor: 'tensorlake/mistral'
    name: 'topic_extraction'
    input_params:
      model_name: 'mistral-large-latest'
      key: 'YOUR_MISTRAL_API_KEY'
      system_prompt: 'Extract the main topics discussed in this debate transcript. List each topic as a brief phrase or title.'
    content_source: 'speech_recognition'
  - extractor: 'tensorlake/mistral'
    name: 'topic_summarization'
    input_params:
      model_name: 'mistral-large-latest'
      key: 'YOUR_MISTRAL_API_KEY'
      system_prompt: 'Summarize the discussion on the main topics from the debate transcript. Provide key points and arguments from both sides.'
    content_source: 'speech_recognition'
"""

extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
client.create_extraction_graph(extraction_graph)
```

Replace `'YOUR_MISTRAL_API_KEY'` with your actual Mistral API key.

OR, if you want to use OpenAI instead of Mistral, replace the Mistral extractor in the graph above with OpenAI extractor like this:

```
  - extractor: 'tensorlake/openai'
    name: 'topic_extraction'
    input_params:
      model: 'gpt-4o-mini'
      api_key: 'YOUR_OPENAI_API_KEY'
      system_prompt: 'Extract the main topics discussed in this debate transcript. List each topic as a brief phrase or title.'
    content_source: 'speech_recognition'
  - extractor: 'tensorlake/openai'
    name: 'topic_summarization'
    input_params:
      model: 'gpt-4o-mini'
      api_key: 'YOUR_OPENAI_API_KEY'
      system_prompt: 'Summarize the discussion on the main topics from the debate transcript. Provide key points and arguments from both sides.'
    content_source: 'speech_recognition'
```

Run this script to set up the pipeline:
```bash
python debate_summary_graph.py
```

## Implementing the Debate Summary Pipeline

Now let's create a script to upload the video and retrieve the summaries. Create a file `upload_and_retreive.py`:

```python
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
    from pytubefix import YouTube

    yt = YouTube("https://www.youtube.com/watch?v=SAL-mNE10TA")
    file_name = "biden_trump_debate_2024.mp4"
    if not os.path.exists(file_name):
        video = yt.streams.filter(progressive=True, file_extension="mp4").order_by("resolution").desc().first()
        video.download(filename=file_name)

    topics, summaries = summarize_debate(file_name)

    print("Debate Topics and Summaries:")
    print(topics, summaries)
```

## Running the Summary Pipeline

To run the debate summary pipeline:

1. Ensure you have the video file of the Biden-Trump 2024 Presidential Debate saved as `biden_trump_debate_2024.mp4` in the same directory as your script.

<img src="https://raw.githubusercontent.com/tensorlakeai/indexify/main/examples/video/transcript/thumbnail.jpg" width="600"/>

2. Run the Python script:
   ```bash
   python debate_summary_pipeline.py
   ```

<img src="https://raw.githubusercontent.com/tensorlakeai/indexify/main/examples/video/transcript/carbon.png" width="600"/>

This will process the video, extract topics, and generate summaries for each topic discussed in the debate.

## Customization and Advanced Usage

You can customize the summarization process by modifying the `system_prompt` in the extraction graph. For example:

- To focus on specific aspects of the debate:
  ```yaml
  system_prompt: 'Summarize the candidate positions and key policy differences on the following topic from the debate transcript:'
  ```

- To generate more concise summaries:
  ```yaml
  system_prompt: 'Provide a brief, bullet-point summary of the main arguments on the following topic from the debate transcript:'
  ```

You can also experiment with different Mistral models by changing the `model_name` parameter to find the best balance between speed and accuracy for your specific use case.

## Conclusion

This debate topic-wise summary pipeline demonstrates the power and flexibility of Indexify for complex, multi-step processing tasks. Key advantages include:

1. **Scalability**: Indexify can handle large video files and process multiple debates efficiently.
2. **Modularity**: Each step in the pipeline (audio extraction, speech recognition, topic extraction, summarization) is separate, allowing for easy customization and improvement.
3. **Error Handling**: Indexify automatically retries failed steps, ensuring robustness in processing.

## Next Steps

- Explore more Indexify features in the [official documentation](https://docs.getindexify.ai)
- Learn about other use cases, such as [entity extraction from documents](https://github.com/mistralai/cookbook/tree/main/third_party/Indexify/pdf-entity-extraction)
- Experiment with different extractors and language models to improve the accuracy and depth of your debate summaries
