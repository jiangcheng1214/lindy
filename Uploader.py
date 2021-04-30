import json
import os
import pyrebase
from Utils import log_warning, log_info


class Uploader:
    def __init__(self):
        self.data_dir_path = os.path.join(os.getcwd(), 'data')
        with open('credentials/firebase_credentials.json', 'r') as f:
            credentials = json.load(f)
        self.firebase = pyrebase.initialize_app(credentials)
        self.storage = self.firebase.storage()
        self.category_codes = ['WOMENBAGSSMALLLEATHERGOODS', 'WOMENSILKSCARVESETC']

    def is_scrape_success(self, timestamp):
        for category_code in self.category_codes:
            flag_path = os.path.join(self.data_dir_path, timestamp, "product", "SUCCESS_" + category_code)
            if not os.path.exists(flag_path):
                log_warning(flag_path + " doesn't exist!")
                return False
        return True

    def upload_products(self, timestamp):
        if not self.is_scrape_success(timestamp):
            log_warning("Not a success scrape with timestamp " + timestamp)
            return False
        for category_code in self.category_codes:
            file_path = os.path.join(self.data_dir_path, timestamp, "product", category_code + ".json")
            dest_path = 'products/{}/{}'.format(timestamp, category_code + ".json")
            self.storage.child(dest_path).put(file_path)
            log_info("{} has been uploaded to {}".format(file_path, dest_path))
        return True
