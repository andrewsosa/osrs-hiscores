import csv
import itertools
from io import StringIO

import pendulum
from metaflow import FlowSpec, JSONType, Parameter, step
from sqlalchemy.orm import Session

from osrs import database
from osrs.remote import SkillRow, yield_skill_rows

GCS_BUCKET = "osrs-hiscores-etl"
TIMESTAMP = str(pendulum.today("UTC"))

# with open("schema/skill_schema.json") as f:
#     from apache_beam.io.gcp import bigquery_tools
#     SKILL_SCHEMA = bigquery_tools.parse_table_schema_from_json(f.read())

# with open("schema/player_schema.json") as f:
#     from apache_beam.io.gcp import bigquery_tools
#     PLAYER_SCHEMA = bigquery_tools.parse_table_schema_from_json(f.read())


MAX_SKILL_PAGE = 200  # 40_000
SKILL_NAMES = [
    "agility",
    "attack",
    "construction",
    "cooking",
    "crafting",
    "defence",
    "farming",
    "firemaking",
    "fishing",
    "fletching",
    "herblore",
    "hitpoints",
    "hunter",
    "magic",
    "mining",
    # "overall",
    "prayer",
    "ranged",
    "runecraft",
    "slayer",
    "smithing",
    "strength",
    "thieving",
    "woodcutting",
]


class Flow(FlowSpec):
    datestamp = Parameter("ds", default=str(pendulum.today().date()))
    pages = Parameter("pages", default=10)
    skills = Parameter("skills", type=JSONType, default=SKILL_NAMES)

    @step
    def start(self):
        # self.skill_pages = list(itertools.product(self.skills, [self.pages]))
        self.next(self.download_page, foreach="skills")

    @step
    def download_page(self):
        skill_name: str = self.input
        target_pages = set(range(1, self.pages + 1))

        with Session(database.make_engine()) as session:
            loaded_pages = set(
                n
                for (n,) in session.query(database.Page.page_number).filter_by(
                    skill_name=skill_name,
                    datestamp=self.datestamp,
                )
            )
            unloaded_pages = target_pages - loaded_pages

            for page_number in unloaded_pages:
                page = database.Page(
                    skill_name=skill_name,
                    page_number=page_number,
                    datestamp=self.datestamp,
                )
                rows = [
                    database.Score(
                        page=page,
                        skill_name=skill_name,
                        player_name=row.name,
                        rank=row.rank,
                        level=row.level,
                        xp=row.xp,
                        timestamp=row.timestamp,
                    )
                    for row in yield_skill_rows(skill_name, page_number)
                ]

                session.add(page)
                session.add_all(rows)
                session.commit()

        self.next(self.join)

    @step
    def join(self, inputs):
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    Flow()
