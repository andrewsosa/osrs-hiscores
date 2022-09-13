```py
# flake8: noqa
import itertools
import json
import logging
import sys
import typing

import apache_beam as beam
import backoff
import pendulum
import requests
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import PipelineOptions
from redis import Redis

GCS_BUCKET = "gs://osrs-hiscores-etl"
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

PAGES = list(range(1, 1 + MAX_SKILL_PAGE))

# parser = argparse.ArgumentParser()
# parser.add_argument("-n", "--num-workers", type=int, default=1)
# parser.add_argument("-p", "--pages", type=int, default=MAX_SKILL_PAGE)
# args, _ = parser.parse_known_args(sys.argv)

beam_options = PipelineOptions(
    # temp_location=f"{GCS_BUCKET}/tmp",
    direct_num_workers=2,
    direct_running_mode="multi_threading",
)


class SkillRow(typing.TypedDict):
    skill: str
    name: str
    rank: int
    level: int
    xp: int
    timestamp: pendulum.DateTime


def basic_skill_row(skill: str, row: dict) -> SkillRow:
    return {
        "skill": skill,
        "name": row["name"],
        "rank": row["rank"],
        "level": row["level"],
        "xp": row["xp"],
        "timestamp": pendulum.now("UTC"),
    }


def player_skill_tuple(row: SkillRow) -> typing.Tuple[str, SkillRow]:
    return (
        row["name"],
        {
            "skill": row["skill"],
            "rank": row["rank"],
            "level": row["level"],
            "xp": row["xp"],
        },
    )


def player_tuple_to_dict(tup):
    return {
        "name": tup[0],
        "skills": tup[1],
    }


def yield_skill_rows(element: tuple, redis: Redis):
    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.RequestException,
        on_backoff=logging.debug,
    )
    def _download_page(skill, page) -> dict:
        response = requests.get(f"http://localhost:8080/skill/{skill}?page={page}")
        response.raise_for_status()
        return response.json()

    skill_name, page_number = element

    page = _download_page(skill_name, page_number)
    yield from (basic_skill_row(skill_name, row) for row in page)

    redis.set(f"{skill_name}:{page_number}", str(pendulum.now("UTC")), ex=60 * 2)


def by_skill(row, num_partitions):
    return SKILL_NAMES.index(row["skill"])


def in_cache(element, redis) -> bool:
    skill, page = element
    return redis.get(f"{skill}:{page}") is not None


# class DoGenerator(beam.DoFn):
#     def __init__(self, generator):
#         self.generator = generator

#     def process(self, *args):
#         yield from self.generator(*args)


def add_timestamp(element):
    unix_ts = element["timestamp"].int_timestamp
    return beam.window.TimestampedValue(element, unix_ts)


class AddWindowingInfoFn(beam.DoFn):
    def process(self, row, window=beam.DoFn.WindowParam):
        yield (id(window), row)


class GetTimestamp(beam.DoFn):
    def process(self, row, timestamp=beam.DoFn.TimestampParam):
        yield "{} - {}".format(timestamp.to_utc_datetime(), row["name"])


class WindowedWritesFn(beam.DoFn):
    """write one file per window/key"""

    def __init__(self, outdir):
        self.outdir = outdir

    def process(self, element):
        (window, elements) = element
        breakpoint()
        window_start = str(window.start.to_utc_datetime()).replace(" ", "_")
        window_end = str(window.end.to_utc_datetime()).replace(" ", "_")
        writer = beam.filesystems.FileSystems.create(
            self.outdir + window_start + "," + window_end + ".txt"
        )

        for row in elements:
            writer.write(str(row) + "\n")

        writer.close()


skill_name = "herblore"

if __name__ == "__main__":
    redis = Redis(host="localhost", port=6379, db=0)

    with beam.Pipeline(options=beam_options) as p:
        (
            p
            | beam.Create(itertools.product([skill_name], PAGES))
            # | beam.Filter(lambda x: not in_cache(x, redis))
            | beam.FlatMap(yield_skill_rows, redis)
            | "Add Timestamps"
            >> beam.Map(
                lambda row: beam.window.TimestampedValue(
                    row, row["timestamp"].int_timestamp
                )
            )
            | "Add Windows"
            >> beam.WindowInto(
                beam.window.FixedWindows(5),
                trigger=beam.trigger.AfterProcessingTime(5),
                accumulation_mode=beam.trigger.AccumulationMode.DISCARDING,
            )
            | "Add Window Info" >> beam.ParDo(AddWindowingInfoFn())
            | "Group By Window" >> beam.GroupByKey()
            # | beam.FlatMap(lambda row: row[1])
            # | beam.Map(lambda x: json.dumps(x, default=str))
            | beam.Map(print)
            # | fileio.WriteToFiles(
            #     "output/",
            #     # f"{GCS_BUCKET}/skills/{TIMESTAMP}/{skill_name}",
            #     shards=1,
            #     # max_writers_per_bundle=0,
            # )
            # | beam.io.WriteToText(
            #     f"{GCS_BUCKET}/skills/{TIMESTAMP}/{skill_name}",
            #     file_name_suffix=".json",
            # )
        )

    # with beam.Pipeline(options=beam_options) as p:
    #     # Download pages for all skills
    #     hiscore_rows = (
    #         p | beam.Create(SKILL_NAMES) | beam.ParDo(DoGenerator(yield_skill_pages))
    #     )

    #     # Partition all rows by skill
    #     skill_partitions = hiscore_rows | beam.Partition(by_skill, len(SKILL_NAMES))

    #     # Write each skill partition as JSON to GCS
    #     for i, partition in enumerate(skill_partitions):
    #         skill_name = SKILL_NAMES[i]
    #         (
    #             partition
    #             | f"Convert {skill_name} to JSON" >> beam.Map(json.dumps)
    #             | f"Upload {skill_name} to GCS"
    #             >> beam.io.WriteToText(
    #                 f"{GCS_BUCKET}/skills/{TIMESTAMP}/{skill_name}",
    #                 file_name_suffix=".json",
    #             )
    #         )

    # Create a BQ Table for each skill
    # for i, partition in enumerate(skill_partitions):
    #     skill_name = SKILL_NAMES[i]
    #     partition | beam.io.WriteToBigQuery(
    #         f"skill_{skill_name}",
    #         dataset="osrs_hiscores",
    #         project="osrs-hiscores-345920",
    #         write_disposition="WRITE_TRUNCATE",
    #         method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
    #         schema=SKILL_SCHEMA,
    #     )

    # Also aggregate all rows by player into one table
    # (
    #     hiscore_rows
    #     | beam.Map(player_skill_tuple)
    #     | beam.GroupByKey()
    #     | beam.Map(player_tuple_to_dict)
    #     # | beam.Map(json.dumps)
    #     # | beam.io.WriteToText(
    #     #     "players",
    #     #     file_name_suffix=".json",
    #     # )
    #     | beam.io.WriteToBigQuery(
    #         "players",
    #         dataset="osrs_hiscores",
    #         project="osrs-hiscores-345920",
    #         write_disposition="WRITE_TRUNCATE",
    #         method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
    #         schema=PLAYER_SCHEMA,
    #     )
    # )
```
