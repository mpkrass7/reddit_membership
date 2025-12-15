# Reddit Membership Fetcher

Pulls Reddit membership counts for specified subreddits and stores them in Databricks.

## Setup

### 1. Local Setup

1. Copy `.env.example` to `.env`:
   ```bash
   cp .env.example .env
   ```

2. Edit `.env` and add your Databricks credentials (Reddit API credentials not needed)

3. Install dependencies:
   ```bash
   uv sync
   ```

4. Run the script:
   ```bash
   uv run reddit_members.py
   ```

### 2. GitHub Actions Setup

Add the following secrets to your GitHub repository:
- Settings → Secrets and variables → Actions → New repository secret

Required secrets:
- `DATABRICKS_SERVER_HOSTNAME`
- `DATABRICKS_CLIENT_ID`
- `DATABRICKS_CLIENT_SECRET`
- `DATABRICKS_HTTP_PATH`

## Usage

Run with default subreddit (Overemployed):
```bash
uv run reddit_members.py
```

Run with specific subreddit:
```bash
uv run reddit_members.py python
```