import json
import os
import pyrebase
from Utils import log_warning, log_info, supported_categories


class Uploader:
    def __init__(self):
        self.data_dir_path = os.path.join(os.getcwd(), 'data', 'scraper')
        with open('credentials/firebase_credentials.json', 'r') as f:
            credentials = json.load(f)
        self.firebase = pyrebase.initialize_app(credentials)
        self.storage = self.firebase.storage()
        self.database = self.firebase.database()
        self.category_codes = supported_categories()

    def is_scrape_success(self, timestamp):
        for category_code in self.category_codes:
            flag_path = os.path.join(self.data_dir_path, timestamp, "product", "SUCCESS_" + category_code)
            if not os.path.exists(flag_path):
                log_warning(flag_path + " doesn't exist!")
                return False
        return True

    def update_latest_timestamp(self, timestamp):
        self.database.child('latest_timestamp').set(timestamp)

    def upload_products(self, timestamp):
        if not self.is_scrape_success(timestamp):
            log_warning("Not a success scrape with timestamp " + timestamp)
            return False
        for category_code in self.category_codes:
            file_path = os.path.join(self.data_dir_path, timestamp, "product", category_code)
            dest_path = 'products/{}/{}'.format(timestamp, category_code)
            self.storage.child(dest_path).put(file_path)
            log_info("{} has been uploaded to {}".format(file_path, dest_path))

        self.update_latest_timestamp(timestamp)
        return True
