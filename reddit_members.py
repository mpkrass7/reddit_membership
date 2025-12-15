# ABOUTME: Script to fetch the number of members (subscribers) in a Reddit community
# ABOUTME: Uses Reddit's public JSON API to fetch subscriber counts without authentication

from datetime import datetime
import requests
import sys


def get_subreddit_members(subreddit_name: str) -> tuple[int, float]:
    """
    Fetch the number of subscribers for a given subreddit.

    Args:
        subreddit_name: Name of the subreddit (without 'r/' prefix)

    Returns:
        Number of subscribers as an integer
    """
    # Use Reddit's public JSON API
    url = f"https://www.reddit.com/r/{subreddit_name}/about.json"
    headers = {"User-Agent": "member_counter/1.0"}

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    data = response.json()
    return data['data']['subscribers'], data['data']['created']


def main():
    # Get subreddit name from command line argument or prompt user
    if len(sys.argv) > 1:
        subreddit_name = sys.argv[1]
    else:
        subreddit_name = "Overemployed"

    # Remove 'r/' prefix if user included it
    subreddit_name = subreddit_name.replace('r/', '')

    try:
        members, created_timestamp = get_subreddit_members(subreddit_name)
        timestamp = datetime.fromtimestamp(created_timestamp).strftime('%Y-%m-%d %H:%M:%S')
        print(f"r/{subreddit_name} has {members:,} members")
        print(f"Subreddit created on: {timestamp}")
    except Exception as e:
        print(f"Error fetching data for r/{subreddit_name}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
