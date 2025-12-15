# ABOUTME: Script to fetch the number of members (subscribers) in a Reddit community
# ABOUTME: Uses Reddit's public JSON API with realistic browser headers to fetch subscriber counts

from datetime import datetime
import os
import sys
import time

from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv
import requests

load_dotenv()


def append_to_databricks_table(table_name: str, data: dict) -> int:
    """
    Append data to a Databricks table using Databricks SDK.

    Args:
        table_name: Name of the Databricks table
        data: Dictionary containing the data to append
    """
    w = WorkspaceClient(
        host=f"https://{os.getenv('DATABRICKS_SERVER_HOSTNAME')}",
        client_id=os.getenv("DATABRICKS_CLIENT_ID"),
        client_secret=os.getenv("DATABRICKS_CLIENT_SECRET"),
    )

    # Build INSERT query with proper value formatting
    columns = ", ".join(data.keys())
    values = ", ".join(
        [f"'{v}'" if isinstance(v, str) else str(v) for v in data.values()]
    )
    sql_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"

    # Extract warehouse ID from HTTP path
    warehouse_id = os.getenv("DATABRICKS_HTTP_PATH").split("/")[-1]

    # Execute statement
    w.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=sql_query, wait_timeout="30s"
    )

    return 200


def get_subreddit_members(subreddit_name: str, retry_count: int = 3) -> tuple[int, float]:
    """
    Fetch the number of subscribers for a given subreddit.

    Args:
        subreddit_name: Name of the subreddit (without 'r/' prefix)
        retry_count: Number of retries if request fails

    Returns:
        Number of subscribers and created date
    """
    # Use old.reddit.com which has less strict anti-bot measures
    url = f"https://old.reddit.com/r/{subreddit_name}/about.json"

    # Create a session to maintain cookies
    session = requests.Session()

    # Headers that mimic a real browser to avoid being blocked
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": f"https://old.reddit.com/r/{subreddit_name}/",
        "DNT": "1",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
    }

    for attempt in range(retry_count):
        try:
            # Add a small delay to avoid rate limiting
            if attempt > 0:
                time.sleep(2 ** attempt)  # Exponential backoff: 2s, 4s, 8s

            # First visit the subreddit page to get cookies
            if attempt == 0:
                session.get(f"https://old.reddit.com/r/{subreddit_name}/", headers=headers, timeout=10)
                time.sleep(1)  # Small delay between requests

            # Now fetch the JSON data
            response = session.get(url, headers=headers, timeout=10)
            response.raise_for_status()

            data = response.json()
            return data["data"]["subscribers"], data["data"]["created"]

        except requests.exceptions.HTTPError as e:
            if attempt == retry_count - 1:
                raise
            print(f"Attempt {attempt + 1} failed: {e}. Retrying...")
        except requests.exceptions.RequestException as e:
            if attempt == retry_count - 1:
                raise
            print(f"Attempt {attempt + 1} failed: {e}. Retrying...")


def main():
    # Get subreddit name from command line argument or prompt user
    if len(sys.argv) > 1:
        subreddit_name = sys.argv[1]
    else:
        subreddit_name = "Overemployed"

    # Remove 'r/' prefix if user included it
    subreddit_name = subreddit_name.replace("r/", "")

    try:
        members, created_timestamp = get_subreddit_members(subreddit_name)
        timestamp = datetime.fromtimestamp(created_timestamp).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        print(f"r/{subreddit_name} has {members:,} members")
        print(f"Subreddit created on: {timestamp}")
        append_to_databricks_table(
            "mk_fiddles.default.reddit_overemployment",
            {
                "membership_count": members,
                "as_of_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "created_date": timestamp,
            },
        )
    except Exception as e:
        print(f"Error fetching data for r/{subreddit_name}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
