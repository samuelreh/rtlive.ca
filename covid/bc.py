import os
import logging
import requests
import pytz
from dateutil.parser import parse as parsedate
from operator import itemgetter

import pandas as pd

from covid.cache import Cache
from covid.models.generative import GenerativeModel
from covid.data import summarize_inference_data

logging.basicConfig(level=logging.CRITICAL)
logging.getLogger("pymc3").setLevel(logging.CRITICAL)

BASE_URL = "http://www.bccdc.ca/Health-Info-Site/Documents"
CASE_URL = f"{BASE_URL}/BCCDC_COVID19_Dashboard_Case_Details.csv"
TEST_URL = f"{BASE_URL}/BCCDC_COVID19_Dashboard_Lab_Information.csv"
BUCKET = os.environ.get('BUCKET', 'rtlive.ca')

def train():
    cache = Cache(BUCKET, "cache", "/tmp/rt-bc")

    cache.download()
    model_has_expired = _raw_data_modified_at() > cache.modified_at()
    if model_has_expired:
        raw_testing, raw_cases = _get_raw_bc_data()
        df = _process_bc_data(raw_testing, raw_cases)
        region = "BC"
        model = GenerativeModel(region, df.loc[region])
        model.sample()
        result = summarize_inference_data(gm.inference_data)
        cache.set({'model': model, 'data': df, 'result': result})
    else:
        model, result = itemgetter('model','result')(cache.get())

    return model, result

def _get_raw_bc_data():
    cases_data = pd.read_csv(CASE_URL)
    testing_data = pd.read_csv(TEST_URL)
    return testing_data, cases_data

def _process_bc_data(raw_testing, raw_cases):
    bc_total_tests = raw_testing[raw_testing.Region=='BC']
    bc_total_cases = raw_cases.groupby('Reported_Date').count()['HA']
    df = pd.merge(bc_total_tests, bc_total_cases, how='left', left_on=['Date'], right_on = ['Reported_Date'])
    df = df.rename(columns={"HA": "positive", "Date": "date", "Region": "region", "New_Tests": "total"})
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
    df['positive'] = df['positive'].fillna(0)
    df = df.set_index(["region","date"])
    return df

def _raw_data_modified_at():
    response = requests.head(CASE_URL)
    return parsedate(response.headers['last-modified']).replace(tzinfo=pytz.UTC)
