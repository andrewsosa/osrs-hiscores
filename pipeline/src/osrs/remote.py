import dataclasses
import logging
import typing

import backoff
import pendulum
import requests


@dataclasses.dataclass
class SkillRow:
    skill: str
    name: str
    rank: int
    level: int
    xp: int
    timestamp: pendulum.DateTime

    def as_dict(self) -> dict:
        return dataclasses.asdict(self)

    @classmethod
    def fields(cls) -> typing.List[str]:
        return [f.name for f in dataclasses.fields(cls)]

    @classmethod
    def from_response(cls, skill_name: str, row: dict):
        return cls(
            skill=skill_name,
            name=row["name"],
            rank=row["rank"],
            level=row["level"],
            xp=row["xp"],
            timestamp=pendulum.now("UTC"),
        )


def yield_skill_rows(
    skill_name: str,
    page_number: int,
) -> typing.Generator[SkillRow, None, None]:
    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.RequestException,
        on_backoff=logging.debug,
    )
    def _download_page(skill, page) -> dict:
        response = requests.get(f"http://localhost:8080/skill/{skill}?page={page}")
        response.raise_for_status()
        return response.json()

    page = _download_page(skill_name, page_number)
    yield from (SkillRow.from_response(skill_name, row) for row in page)
