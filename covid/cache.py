import os
from pathlib import Path
import pickle
import boto3
from datetime import datetime
from pathlib import Path
import pytz


class Cache:
    FILENAME = "model.pkl"

    def __init__(self, s3_bucket, s3_base_path, local_base_path):
        self.s3 = boto3.client("s3")
        self.s3_bucket = s3_bucket
        self.s3_path = f"{s3_base_path}/{self.FILENAME}"
        self.local_path = f"{local_base_path}/{self.FILENAME}"
        self.local_base_path = local_base_path
        Path(self.local_base_path).mkdir(parents=True, exist_ok=True)

    def download(self):
        try:
            self.s3.download_file(self.s3_bucket, self.s3_path, self.local_path)
        except Exception as e:
            return

    def modified_at(self):
        try:
            model_modified_at = self.s3.head_object(
                Bucket=self.s3_bucket, Key=self.s3_path
            )["LastModified"]
        except self.s3.exceptions.ClientError:
            model_modified_at = datetime.fromtimestamp(0)
        return model_modified_at.replace(tzinfo=pytz.UTC)

    def set(self, data):
        with open(self.local_path, "wb") as buff:
            pickle.dump(data, buff)
        return self.s3.upload_file(self.local_path, self.s3_bucket, self.s3_path)

    def get(self):
        return pickle.load(open(self.local_path, "rb"))
