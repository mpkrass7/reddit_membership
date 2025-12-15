# ABOUTME: Script to fetch the number of members (subscribers) in a Reddit community
# ABOUTME: Uses Reddit's public JSON API to fetch subscriber counts without authentication

from datetime import datetime
import os
import sys

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


def get_subreddit_members(subreddit_name: str) -> tuple[int, float]:
    """
    Fetch the number of subscribers for a given subreddit.

    Args:
        subreddit_name: Name of the subreddit (without 'r/' prefix)

    Returns:
        Number of subscribers and created date
    """
    # Use Reddit's public JSON API
    url = f"https://www.reddit.com/r/{subreddit_name}/about.json"
    headers = {"User-Agent": "member_counter/1.0"}

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    data = response.json()
    return data["data"]["subscribers"], data["data"]["created"]


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
