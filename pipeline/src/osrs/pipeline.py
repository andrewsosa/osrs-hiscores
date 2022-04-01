import json

import apache_beam as beam
import requests
from apache_beam.options.pipeline_options import PipelineOptions

MAX_PAGE = 3
SKILL_NAMES = {
    "attack",
    "defence",
    "strength",
    "hitpoints",
    "ranged",
    "prayer",
    "magic",
    "cooking",
}


def yield_skill_pages(skill_name: str):
    for i in range(1, MAX_PAGE + 1):
        response = requests.get(f"http://localhost:8080/skill/{skill_name}?page={i}")
        rows = response.json()
        yield from ({**row, "skill": skill_name} for row in rows)


class DoGenerator(beam.DoFn):
    def __init__(self, generator):
        self.generator = generator

    def process(self, element):
        yield from self.generator(element)


with beam.Pipeline(options=PipelineOptions()) as p:
    (
        p
        | beam.Create(SKILL_NAMES)
        | beam.ParDo(DoGenerator(yield_skill_pages))
        | beam.Map(lambda s: json.dumps(s))
        | beam.io.WriteToText("output", file_name_suffix=".json")
    )
