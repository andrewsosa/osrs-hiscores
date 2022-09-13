import csv
import itertools
from io import StringIO

import pendulum
from google.cloud import storage
from metaflow import FlowSpec, JSONType, Parameter, step

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
        self.skill_pages = list(itertools.product(self.skills, [self.pages]))
        self.next(self.download_page, foreach="skill_pages")

    @step
    def download_page(self):
        skill_name: str
        page_number: int
        skill_name, page_number = self.input

        buffer = StringIO()
        writer = csv.DictWriter(buffer, SkillRow.fields())

        for row in yield_skill_rows(skill_name, page_number):
            writer.writerow(row.as_dict())

        buffer.seek(0)

        bucket = storage.Client().bucket(GCS_BUCKET)
        blob = bucket.blob(f"{self.datestamp}/{skill_name}/{page_number}.csv")
        blob.upload_from_string(buffer.getvalue(), content_type="text/csv")

        self.next(self.join)

    @step
    def join(self, inputs):
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    Flow()
