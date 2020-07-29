import os
import logging
import requests
import pytz
from datetime import timedelta, datetime
from dateutil.parser import parse as parsedate
from operator import itemgetter

import pandas as pd

from covid.cache import Cache
from covid.models.generative import GenerativeModel
from covid.data import summarize_inference_data

logging.basicConfig(level=logging.CRITICAL)
logging.getLogger("pymc3").setLevel(logging.CRITICAL)

BASE_URL = "https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov"
CASE_URL = f"{BASE_URL}/cases_timeseries_prov.csv"
TEST_URL = f"{BASE_URL}/testing_timeseries_prov.csv"
BUCKET = os.environ.get('BUCKET', 'rtlive.ca')
REGION = "Alberta"

def train():
    cache = Cache(BUCKET, "cache/alberta", "/tmp/rt-alberta")

    model_has_expired = datetime.now(pytz.UTC) > cache.modified_at() + timedelta(hours=20)
    if model_has_expired:
        raw_testing, raw_cases = _get_raw_data()
        df = _process_data(raw_testing, raw_cases)
        model = GenerativeModel(REGION, df.loc[REGION])
        model.sample()
        result = summarize_inference_data(model.inference_data)
        cache.set({'model': model, 'data': df, 'result': result})
    else:
        cache.download()
        model, result = itemgetter('model','result')(cache.get())

    return model, result

def _get_raw_data():
    cases_data = pd.read_csv(CASE_URL)
    testing_data = pd.read_csv(TEST_URL)
    return testing_data, cases_data

def _process_data(raw_testing, raw_cases):
    total_cases = raw_cases[raw_cases.province==REGION]
    total_tests = raw_testing[raw_testing.province==REGION]
    df = pd.merge(total_tests, total_cases, how='left', left_on=['date_testing'], right_on = ['date_report'])
    df = df.rename(columns={"cases": "positive", "date_report": "date", "province_x": "region", "testing": "total"})
    df["date"] = pd.to_datetime(df["date"], format="%d-%m-%Y")
    df['positive'] = df['positive'].fillna(0)
    df = df.set_index(["region","date"])
    return df
