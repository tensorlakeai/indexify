import logging
import os
from typing import Dict, List
from pydantic import BaseModel, Field
from indexify import RemoteGraph
from indexify.functions_sdk.graph import Graph
from indexify.functions_sdk.indexify_functions import indexify_function
from indexify.functions_sdk.image import Image

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Data Models
class Tweets(BaseModel):
    tweets: List[str] = Field(default_factory=list, description="List of generated tweets")

class RankedTweets(BaseModel):
    scores: List[float] = Field(description="List of scores for the tweets. Higher score means better tweet.")

# Define custom image

base_image = (
    Image()
    .name("tensorlake/base-image")
)
openai_image = (
    Image()
    .name("tensorlake/openai-image")
    .run("pip install openai")
)

@indexify_function(image=openai_image)
def generate_tweet_topics(subject: str) -> List[str]:
    """Generate topics for tweets about a given subject."""
    import openai
    from pydantic import BaseModel, Field
    from typing import List
    client = openai.OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

    class Topics(BaseModel):
        topics: List[str] = Field(default_factory=list)

    response = client.chat.completions.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You are a helpful assistant that generates topics for a tweet about a given subject."},
            {"role": "user", "content": f"Generate 5 topics for a tweet about {subject}"},
        ],
        response_model=Topics
    )
    topics = response.choices[0].message.content
    return topics.topics

@indexify_function(image=openai_image)
def generate_tweet(topic: str) -> str:
    """Generate a tweet about a given topic."""
    import openai
    from pydantic import BaseModel, Field
    client = openai.OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

    class Tweet(BaseModel):
        tweet: str = Field(description="a tweet about the given topic")

    response = client.chat.completions.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You are a helpful assistant that generates a tweet about a given topic."},
            {"role": "user", "content": f"Generate a tweet about {topic}"},
        ],
        response_model=Tweet
    )
    tweet = response.choices[0].message.content
    return tweet.tweet

@indexify_function(image=base_image,accumulate=Tweets)
def accumulate_tweets(acc: Tweets, tweet: str) -> Tweets:
    """Accumulate generated tweets."""
    acc.tweets.append(tweet)
    return acc

@indexify_function(image=openai_image)
def score_and_rank_tweets(tweets: Tweets) -> RankedTweets:
    """Score and rank the accumulated tweets."""
    import openai
    client = openai.OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    tweet_contents = "\n".join(tweets.tweets)
    response = client.chat.completions.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You are a helpful assistant that scores and ranks tweets based on their relevance to a given topic."},
            {"role": "user", "content": f"Score and rank the following tweets, separated by new lines: {tweet_contents}"},
        ],
        response_model=RankedTweets
    )
    ranked_tweets = response.choices[0].message.content
    return ranked_tweets

def create_tweets_graph():
    graph = Graph(name="tweet-gen", description="generate tweets", start_node=generate_tweet_topics)
    graph.add_edge(generate_tweet_topics, generate_tweet)
    graph.add_edge(generate_tweet, accumulate_tweets)
    graph.add_edge(accumulate_tweets, score_and_rank_tweets)
    return graph

def deploy_graphs(server_url: str):
    graph = create_tweets_graph()
    RemoteGraph.deploy(graph, server_url=server_url)
    logging.info("Graph deployed successfully")

def run_workflow(mode: str, server_url: str = 'http://localhost:8900'):
    if mode == 'in-process-run':
        graph = create_tweets_graph()
    elif mode == 'remote-run':
        graph = RemoteGraph.by_name("tweet-gen", server_url=server_url)
    else:
        raise ValueError("Invalid mode. Choose 'in-process-run' or 'remote-run'.")

    import httpx
    subject = httpx.get("https://discord.com/blog/how-discord-reduced-websocket-traffic-by-40-percent").text
    logging.info(f"Generating tweets for subject: {subject[:100]}...")

    invocation_id = graph.run(block_until_done=True, subject=subject)
    
    score_results = graph.output(invocation_id, "score_and_rank_tweets")
    tweet_scores = score_results[0].scores

    tweets_result = graph.output(invocation_id, "accumulate_tweets")
    tweets = tweets_result[0].tweets

    for tweet, score in zip(tweets, tweet_scores):
        print(f"Tweet: {tweet}\nScore: {score}\n")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run Tweet Generator")
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
