from typing import Dict, List

from pydantic import BaseModel, Field

from indexify import GraphDS, create_client, indexify_function


class Tweets(BaseModel):
    tweets: List[str] = Field(default_factory=list)


@indexify_function()
def generate_tweet_topics(subject: str) -> List[str]:
    import openai

    client = openai.OpenAI()

    class Topics(BaseModel):
        topics: List[str] = Field(default_factory=list)

    response = client.beta.chat.completions.parse(
        model="gpt-4o-2024-08-06",
        messages=[
            {
                "role": "system",
                "content": "You are a helpful assistant that generates topics for a tweet about a given subject.",
            },
            {
                "role": "user",
                "content": f"Generate 5 topics for a tweet about {subject}",
            },
        ],
        response_format=Topics,
    )
    topics = response.choices[0].message.parsed
    return topics.topics


@indexify_function()
def generate_tweet(topic: str) -> str:
    import openai

    client = openai.OpenAI()

    class Tweet(BaseModel):
        tweet: str = Field(description="a tweet about the given topic")

    response = client.beta.chat.completions.parse(
        model="gpt-4o-2024-08-06",
        messages=[
            {
                "role": "system",
                "content": "You are a helpful assistant that generates a tweet about a given topic.",
            },
            {"role": "user", "content": f"Generate a tweet about {topic}"},
        ],
        response_format=Tweet,
    )
    tweet = response.choices[0].message.parsed
    return tweet.tweet


@indexify_function(accumulate=Tweets)
def accumulate_tweets(acc: Tweets, tweet: str) -> Tweets:
    acc.tweets.append(tweet)
    return acc


class RankedTweets(BaseModel):
    scores: List[float] = Field(
        description="a list of scores for the tweets. The higher the score, the better the tweet."
    )


@indexify_function()
def score_and_rank_tweets(tweets: Tweets) -> RankedTweets:
    import openai

    client = openai.OpenAI()
    tweet_contents = "\n".join(tweets.tweets)
    response = client.beta.chat.completions.parse(
        model="gpt-4o-2024-08-06",
        messages=[
            {
                "role": "system",
                "content": "You are a helpful assistant that scores and ranks tweets based on their relevance to a given topic.",
            },
            {
                "role": "user",
                "content": f"Score and rank the following tweets, separated by new lines: {tweet_contents}",
            },
        ],
        response_format=RankedTweets,
    )
    ranked_tweets = response.choices[0].message.parsed
    return ranked_tweets


def create_tweets_graph():
    graph = GraphDS(
        name="tweet-gen",
        description="generate tweets",
        start_node=generate_tweet_topics,
    )
    graph.add_edge(generate_tweet_topics, generate_tweet)
    graph.add_edge(generate_tweet, accumulate_tweets)
    graph.add_edge(accumulate_tweets, score_and_rank_tweets)
    return graph


if __name__ == "__main__":
    graph = create_tweets_graph()
    client = create_client(local=True)
    client.register_compute_graph(graph)
    import httpx

    subject = httpx.get(
        "https://discord.com/blog/how-discord-reduced-websocket-traffic-by-40-percent"
    ).text
    invocation_id = client.invoke_graph_with_object(graph.name, subject=subject)
    score_results = client.graph_outputs(
        graph.name, invocation_id=invocation_id, fn_name="score_and_rank_tweets"
    )
    tweet_scores = score_results[0].scores
    tweets_result = client.graph_outputs(
        graph.name, invocation_id=invocation_id, fn_name="accumulate_tweets"
    )
    tweets = tweets_result[0].tweets
    for tweet, score in zip(tweets, tweet_scores):
        print(f"Tweet: {tweet}\nScore: {score}\n\n")
