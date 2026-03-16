import requests

resp = requests.post(
    "https://ava.andrew-chat.com/api/v1/user",
    json={"username": "krishna", "password": "889134jk&$2"},
    timeout=30,
)
resp.raise_for_status()
data = resp.json()
print("user_id:", data["user_id"])
print("token:", data["authorization"])



import json
from pathlib import Path

path = Path("KPIs/large_seed_res/buyer_quarter_vector_docs.jsonl")
with path.open("r", encoding="utf-8") as f:
    first_line = f.readline().strip()
doc = json.loads(first_line)
sample_text = doc["embedding_text"]
print(sample_text[:500])  # sanity check


import json
import requests
from pathlib import Path

AVA_TOKEN = "mVzFeN1mrWETv87grunbxpkaC-bd8OXMXNKLua4Rdqs"

# 1) Load one embedding_text from your JSONL
path = Path("KPIs/large_seed_res/buyer_quarter_vector_docs.jsonl")
with path.open("r", encoding="utf-8") as f:
    first_line = f.readline().strip()
doc = json.loads(first_line)
text = doc["embedding_text"]

# 2) Call Ava embeddings endpoint
resp = requests.post(
    "https://ava.andrew-chat.com/api/v1/embeddings",
    headers={
        "Authorization": AVA_TOKEN,  # raw token, no "Bearer "
        "Content-Type": "application/json",
    },
    json={
        "texts": [text],
        "type": "RETRIEVAL_DOCUMENT",  # or SEMANTIC_SIMILARITY
    },
    timeout=60,
)
resp.raise_for_status()
data = resp.json()

vec = data["embeddings_matrix"][0]
print("First vector length (dimension):", len(vec))
print("First few values:", vec[:5])