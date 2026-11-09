"""Send a Discord notification when a new paper has been processed.
"""

import os

import requests

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")


def notify_discord(filename: str, meta: dict) -> None:
    """Post a short message with the paper's metadata to a Discord channel."""
    if not DISCORD_WEBHOOK_URL:
        raise ValueError("DISCORD_WEBHOOK_URL is not set: no notification sent.")

    message = {
        "content": "📄 A new paper has been added to the study library!",
        "embeds": [
            {
                "title": meta["title"][:256],                # Discord limit: 256 characters
                "description": meta["summary"][:4000],       # Discord limit: 4096 characters
                "fields": [
                    {"name": "Category", "value": meta["category"][:1024], "inline": True},
                    {"name": "File", "value": filename[:1024], "inline": True},
                    {"name": "Keywords", "value": (", ".join(meta["keywords"]) or "-")[:1024]},
                ],
            }
        ],
    }
    response = requests.post(DISCORD_WEBHOOK_URL, json=message, timeout=10)
    response.raise_for_status()  # error if Discord refuses the message (Airflow will show the task as failed)
    print(f"Discord notification sent for {filename}")


if __name__ == "__main__":
    # Quick manual test: python dags/discord_notify.py
    notify_discord(
        "test.pdf",
        {
            "title": "Test paper",
            "summary": "This is a test notification from our Airflow pipeline.",
            "keywords": ["test", "airflow"],
            "category": "Data Engineering",
        },
    )
