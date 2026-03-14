import os
import sys
from dotenv import load_dotenv


def load_config():
    load_dotenv()

    required = ["TWITCH_CLIENT_ID", "TWITCH_CLIENT_SECRET"]
    missing = [k for k in required if not os.getenv(k)]

    if missing:
        print(f"FATAL: Missing environment variables: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)

    return {
        "client_id": os.getenv("TWITCH_CLIENT_ID"),
        "client_secret": os.getenv("TWITCH_CLIENT_SECRET"),
    }
