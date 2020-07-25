import os
from pathlib import Path
import pickle
import boto3
from datetime import datetime
from pathlib import Path
import pytz

class Cache():
    FILENAME = 'model.pkl'

    def __init__(self, s3_bucket, s3_base_path, local_base_path):
        self.s3 = boto3.client('s3')
        self.s3_bucket = s3_bucket
        self.s3_path = f"{s3_base_path}/{self.FILENAME}"
        self.local_path = f"{local_base_path}/{self.FILENAME}"
        self.local_base_path = local_base_path

    def download(self):
        Path(self.local_base_path).mkdir(parents=True, exist_ok=True)
        if not os.path.exists(self.local_path):
            try:
                self.s3.download_file(self.s3_bucket, self.s3_path, self.local_path)
            except Exception as e:
                return

    def modified_at(self):
        if os.path.exists(self.local_path):
            model_timestamp = os.path.getmtime(self.local_path)
        else:
            model_timestamp = 0
        return datetime.fromtimestamp(model_timestamp).replace(tzinfo=pytz.UTC)

    def set(self, data):
        with open(local_path, 'wb') as buff:
            pickle.dump(data, buff)
        response = self.s3.upload_file(self.local_path, self.bucket, self.s3_path)

    def get(self):
        return pickle.load(open(self.local_path, 'rb'))
