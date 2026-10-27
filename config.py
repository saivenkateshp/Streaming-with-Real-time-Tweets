import os

# Twitter / X API v2 — read from the environment, never hard-code secrets.
# export TWITTER_BEARER_TOKEN="your-token-here"   (or put it in a local .env file)
bearer_token = os.environ.get("TWITTER_BEARER_TOKEN")
if not bearer_token:
    raise RuntimeError("TWITTER_BEARER_TOKEN is not set. See .env.example.")

# Kafka settings
kafka_url = os.environ.get("KAFKA_URL", "localhost:9092")
topic_name = os.environ.get("KAFKA_TOPIC", "tweets")

# Elasticsearch settings
host = os.environ.get("ES_HOST", "localhost")
port = int(os.environ.get("ES_PORT", "9200"))
elastic_url = f"https://{host}:{port}"
