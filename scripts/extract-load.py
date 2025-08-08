import requests
import pandas as pd
import duckdb
import datetime
import os
from dotenv import load_dotenv

# Load access token from .env file
load_dotenv()
access_token = os.getenv("ACCESS_TOKEN")
print("Access token loaded:", access_token if access_token else "⚠️ NOT FOUND")



# Set API endpoint and headers
API_URL = "https://api.producthunt.com/v2/api/graphql"
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}

# Build GraphQL query to fetch today’s posts
today = datetime.date.today().isoformat()
query = f"""
{{
  posts(order: VOTES, postedAfter: "{today}") {{
    edges {{
      node {{
        name
        tagline
        votesCount
        commentsCount
        createdAt
        url
        topics {{
          edges {{
            node {{
              name
            }}
          }}
        }}
      }}
    }}
  }}
}}
"""

# Send request
response = requests.post(API_URL, json={"query": query}, headers=headers)
if response.status_code != 200:
    raise Exception(f"Request failed: {response.status_code} - {response.text}")

# Parse results into a DataFrame
posts = response.json()["data"]["posts"]["edges"]
rows = []
for post in posts:
    node = post["node"]
    topics = [t["node"]["name"] for t in node["topics"]["edges"]]
    rows.append({
        "name": node["name"],
        "tagline": node["tagline"],
        "votes_count": node["votesCount"],
        "comments_count": node["commentsCount"],
        "created_at": node["createdAt"],
        "url": node["url"],
        "topics": ", ".join(topics)
    })

df = pd.DataFrame(rows)
print(df.head())

# Store results into DuckDB
conn = duckdb.connect("data/producthunt.duckdb")
conn.execute("CREATE OR REPLACE TABLE products AS SELECT * FROM df")
print("✅ Data written to DuckDB.")
