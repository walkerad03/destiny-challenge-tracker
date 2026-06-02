import json
import time
from pathlib import Path
from typing import List

import requests


def fetch_character_activity_history(
    membership_type: int,
    account_id: str,
    character_id: str,
    api_key: str,
    staging_dir: str = "/tmp/bronze_staging",
) -> List[str]:

    base_url = "https://www.bungie.net/Platform"
    headers = {"X-API-Key": api_key}

    output_path = Path(staging_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    saved_files: List[str] = []
    page = 0
    count = 50
    mode = "raid"

    while True:
        url = (
            f"{base_url}/Destiny2/{membership_type}/Account/{account_id}/"
            f"Character/{character_id}/Stats/Activities/"
            f"?count={count}&mode={mode}&page={page}"
        )

        response = requests.get(url, headers=headers)

        response.raise_for_status()

        data = response.json()

        activities = data.get("Response", {}).get("activities")
        if not activities:
            break

        filename = f"history_{account_id}_{character_id}_raid_page_{page}.json"
        file_path = output_path / filename

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

        saved_files.append(str(file_path))

        page += 1

        time.sleep(0.25)

    return saved_files
