import json
import time
from pathlib import Path
from typing import List

import requests

from common.destiny_enums import ActivityMode


def fetch_character_activity_history(
    membership_type: int,
    account_id: str,
    character_id: str,
    api_key: str,
    staging_dir: str = "/tmp/bronze_staging",
) -> List[str]:
    output_path = Path(staging_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    all_saved_files = []
    modes = [ActivityMode.RAID, ActivityMode.DUNGEON]

    for mode in modes:
        files = _get_activities(
            membership_type,
            account_id,
            character_id,
            api_key,
            mode,
            output_path,
        )
        all_saved_files.extend(files)

    return all_saved_files


def _query_endpoint(url, api_key):
    headers = {"X-API-Key": api_key}
    res = requests.get(url, headers=headers)
    res.raise_for_status()
    return res.json()


def _get_activities(
    membership_type: int,
    account_id: str,
    character_id: str,
    api_key: str,
    mode: int,
    output_path: Path,
) -> list[str]:
    page = 0
    count = 50
    saved_files = []

    while True:
        url = (
            "https://www.bungie.net/Platform/"
            f"Destiny2/{membership_type}/Account/{account_id}/"
            f"Character/{character_id}/Stats/Activities/"
            f"?count={count}&mode={mode}&page={page}"
        )

        data = _query_endpoint(url, api_key)

        activities = data.get("Response", {}).get("activities")
        if not activities:
            break

        filename = (
            f"history_{account_id}_{character_id}_{mode}_page_{page}.json"
        )
        file_path = output_path / filename

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

        saved_files.append(str(file_path))

        page += 1
        time.sleep(0.1)

    return saved_files
