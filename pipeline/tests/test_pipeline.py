import re
from unittest.mock import MagicMock

import freezegun
import responses

from osrs.remote import yield_skill_rows


@freezegun.freeze_time("2020-01-01")
@responses.activate
def test_yield_skill_pages():
    for status_code in [502, 502, 502, 200]:
        responses.add(
            responses.GET,
            re.compile(r".*"),
            status=status_code,
            json=[
                {
                    "dead": False,
                    "level": 1,
                    "name": "dnddru",
                    "rank": 1,
                    "xp": 0,
                }
            ],
        )

    results = list(yield_skill_rows(("herblore", 1), MagicMock()))

    assert results == [
        {
            "skill": "herblore",
            "name": "dnddru",
            "rank": 1,
            "level": 1,
            "xp": 0,
            "timestamp": "2020-01-01T00:00:00+00:00",
        }
    ]
