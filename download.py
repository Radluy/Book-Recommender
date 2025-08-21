import json
import os
from pathlib import Path
from zipfile import ZipFile

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By


DATASET_NAME = "arashnic/book-recommendation-dataset"
DATASETS_DIR = Path(__file__).parent / "datasets"
KAGGLE_SECRET_PATH = Path("kaggle_secrets.json")
SHAREPOINT_URL = "https://datasentics.sharepoint.com/:u:/s/EXTDataEngineerTask/EbeD2T97tahCitR2aWOh__8BpCy1DJX8lrBOPBgF6VZulQ?e=N2c06o"
HEADERS = {"User-Agent": "Mozilla/5.0"}


def setup_kaggle():
    # Set kaggle api key as env variables.
    if not KAGGLE_SECRET_PATH.exists():
        raise FileNotFoundError(f"File with kaggle api key not found at: \"{str(KAGGLE_SECRET_PATH)}\"")

    with KAGGLE_SECRET_PATH.open() as f:
        secret = json.load(f)
    os.environ['KAGGLE_USERNAME'] = secret['username']
    os.environ['KAGGLE_KEY'] = secret['key']

def get_kaggle_connection():
    """Init Kaggle Api connection and authenticate."""
    setup_kaggle()
    import kaggle
    api = kaggle.KaggleApi()
    api.authenticate()
    return api

def download_data():
    """Download book recommendation dataset from Kaggle"""
    api = get_kaggle_connection()
    if not DATASETS_DIR.exists():
        api.dataset_download_files(DATASET_NAME, path=DATASETS_DIR, unzip=True)


def download_from_sharepoint():
    """
        Download datasets from the DataScentics Sharepoint.
        WARNING: do not use without DataScentics permission!!!
    """
    # download by clicking button on sharepoint
    options = Options()
    options.add_argument("--headless") 
    driver = webdriver.Chrome(options=options)
    driver.get(SHAREPOINT_URL)
    driver.implicitly_wait(10)
    element = driver.find_element(By.XPATH, '//*[@id="downloadCommand"]')
    element.click()
    driver.quit()

    # extract from zip, move to datasets/ 
    downloads_dir = Path('~/Downloads').expanduser()
    for file in downloads_dir.glob(r'book_recommender.*'):
        if file.suffix == '.zip':
            zip_file = ZipFile(str(file))
            zip_file.extractall(DATASETS_DIR.absolute())


if __name__ == '__main__':
    download_data()
    # download_from_sharepoint()