import os
import logging
import requests
import pytz
from dataclasses import dataclass
from datetime import timedelta, datetime
from dateutil.parser import parse as parsedate
from operator import itemgetter

import pandas as pd

from covid import data_processor
from covid.cache import Cache
from covid.models.generative import GenerativeModel
from covid.data import summarize_inference_data

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.CRITICAL)
logging.getLogger("pymc3").setLevel(logging.CRITICAL)


BUCKET = os.environ.get("BUCKET", "rtlive.ca")
START_DATE = os.environ.get("START_DATE", "01-01-2000")
FORCE_USE_CACHE: bool = str.lower(os.environ.get("FORCE_USE_CACHE", "false")) == "true"
SET_CACHE: bool = str.lower(os.environ.get("SET_CACHE", "true")) == "true"


@dataclass
class Summary:
    cases: int
    tests: int
    current: float
    updated_at: datetime


TIMEZONES = {
    "BC": "Canada/Pacific",
    "Ontario": "Canada/Eastern",
    "Quebec": "Canada/Eastern",
    "Alberta": "Canada/Mountain",
}


def train(region):
    processor = data_processor.factory(region)
    cache = Cache(BUCKET, f"cache/{region}", f"/tmp/rt-{region}")
    timezone = pytz.timezone(TIMEZONES[region])

    if FORCE_USE_CACHE or datetime.now(pytz.UTC) > (
        cache.modified_at() + timedelta(hours=20)
    ):
        cache.download()
        model, result = itemgetter("model", "result")(cache.get())
        updated_at = cache.modified_at().astimezone(timezone)
        summary = _build_summary(result, updated_at)
        return model, result, summary

    df = processor.process_data(START_DATE)
    model = GenerativeModel(region, df.loc[region])
    model.sample()
    result = summarize_inference_data(model.inference_data)
    if SET_CACHE:
        cache.set({"model": model, "data": df, "result": result})

    updated_at = datetime.now().astimezone(timezone)
    summary = _build_summary(result, updated_at)
    return model, result, summary


def _build_summary(result, updated_at):
    cases = int(result.positive.sum())
    tests = int(result.tests.sum())
    current = result["median"].iloc[-1]
    return Summary(cases, tests, current, updated_at)
