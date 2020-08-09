from dataclasses import dataclass

import pandas as pd


def factory(region: str):
    if region == "BC":
        return BCDataProcessor()

    return CanadaDataProcessor(region)


@dataclass
class CanadaDataProcessor:
    region: str

    BASE_URL = "https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov"
    CASE_URL = f"{BASE_URL}/cases_timeseries_prov.csv"
    TEST_URL = f"{BASE_URL}/testing_timeseries_prov.csv"

    def process_data(self, start_date):
        raw_cases = pd.read_csv(self.CASE_URL)
        raw_testing = pd.read_csv(self.TEST_URL)
        total_cases = raw_cases[raw_cases.province == self.region]
        total_tests = raw_testing[raw_testing.province == self.region]
        df = pd.merge(
            total_tests,
            total_cases,
            how="left",
            left_on=["date_testing"],
            right_on=["date_report"],
        )
        df = df.rename(
            columns={
                "cases": "positive",
                "date_report": "date",
                "province_x": "region",
                "testing": "total",
            }
        )
        df["date"] = pd.to_datetime(df["date"], format="%d-%m-%Y")
        df = df[df.date > pd.to_datetime(start_date, format="%d-%m-%Y")]
        df["positive"] = df["positive"].fillna(0)
        df.loc[df["total"] < 0, "total"] = 0
        df = df.set_index(["region", "date"])
        return df


class BCDataProcessor:
    region: str = "BC"

    BASE_URL = "http://www.bccdc.ca/Health-Info-Site/Documents"
    CASE_URL = f"{BASE_URL}/BCCDC_COVID19_Dashboard_Case_Details.csv"
    TEST_URL = f"{BASE_URL}/BCCDC_COVID19_Dashboard_Lab_Information.csv"

    def process_data(self, start_date):
        raw_cases = pd.read_csv(self.CASE_URL)
        raw_testing = pd.read_csv(self.TEST_URL)
        bc_total_tests = raw_testing[raw_testing.Region == self.region]
        bc_total_cases = raw_cases.groupby("Reported_Date").count()["HA"]
        df = pd.merge(
            bc_total_tests,
            bc_total_cases,
            how="left",
            left_on=["Date"],
            right_on=["Reported_Date"],
        )
        df = df.rename(
            columns={
                "HA": "positive",
                "Date": "date",
                "Region": "region",
                "New_Tests": "total",
            }
        )
        df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
        df = df[df.date > pd.to_datetime(start_date, format="%d-%m-%Y")]
        df["positive"] = df["positive"].fillna(0)
        df = df.set_index(["region", "date"])
        return df
