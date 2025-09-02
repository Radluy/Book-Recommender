import pandas as pd

from download import download_data
from recommender import BOOKS_DATA, DATASETS_DIR, PROCESSED_DATA, RATINGS_DATA


def validate_isbn(isbn) -> bool:
    """Validates that isbn is in the correct format"""
    sum = 0
    for idx, char in enumerate(isbn):
        if char == 'X':
            char = 10
        sum += int(char) * (10 - idx)
    return sum % 11 == 0

def fix_encoding(s):
    """Fixes incorrectly loaded rows encoded in latin1."""
    try:
        return s.encode("latin1").decode("utf-8")
    except:
        return s

def clean_books_data(raw_dataset: pd.DataFrame) -> pd.DataFrame:
    """Cleans data from books dataset. Removes empty or incorrect values."""
    dataset = raw_dataset.apply(lambda x: x.str.lower() if (x.dtype == 'object') else x)
    dataset['Book-Title'] = dataset['Book-Title'].apply(fix_encoding)
    dataset = dataset.drop_duplicates('Book-Title')
    dataset = dataset[~dataset['Book-Author'].isna() | dataset['Book-Title'].isna() | dataset['ISBN'].isna()]
    # remove unnecessary columns
    dataset = dataset.drop(['Image-URL-S', 'Image-URL-M', 'Image-URL-L'], axis=1)
    dataset = dataset.drop(['Year-Of-Publication', 'Publisher'], axis=1)
    # ISBN validity check
    # each digit is multiplied by weights: 10, 9, 8...1
    # sum of these values divided by 11 should be without remainder
    # if the last digit is X, it represents a 10
    dataset = dataset[dataset["ISBN"].astype(str).str.fullmatch(r"[0-9X]+")]
    dataset = dataset[dataset['ISBN'].apply(lambda x: validate_isbn(x))]
    
    return dataset

def clean_ratings_data(raw_dataset: pd.DataFrame) -> pd.DataFrame:
    """Cleans data from the ratings dataset. Removes empty or incorrect values."""
    # multiple ratings of one person for the same book
    dataset = raw_dataset.drop_duplicates(['ISBN', 'User-ID'])
    # implicit value
    dataset = dataset[dataset['Book-Rating'] != 0]
    dataset = dataset[(dataset['Book-Rating'] > 0) & (dataset['Book-Rating'] <= 10)]
    # weird ISBN found - must be digits or 'x'
    dataset = dataset[dataset["ISBN"].astype(str).str.fullmatch(r"[0-9X]+")]
    dataset = dataset[dataset['ISBN'].apply(lambda x: validate_isbn(x))]

    return dataset

def load_dataset(from_scratch=False) -> pd.DataFrame:
    """Loads data either from stored pickle or from raw csv files"""
    if PROCESSED_DATA.exists() and not from_scratch:
        dataset: pd.DataFrame = pd.read_pickle(PROCESSED_DATA)
        return dataset

    # load ratings
    if not DATASETS_DIR.exists() or not any(DATASETS_DIR.iterdir()):
        download_data()
    ratings: pd.DataFrame = pd.read_csv(RATINGS_DATA, encoding='utf-8', sep=',', low_memory=False)
    ratings = clean_ratings_data(ratings)

    # load books
    books: pd.DataFrame = pd.read_csv(BOOKS_DATA, encoding='utf-8', sep=',', on_bad_lines='skip', low_memory=False)
    books = clean_books_data(books)


    dataset: pd.DataFrame = pd.merge(ratings, books, on=['ISBN'])

    dataset.to_pickle(PROCESSED_DATA)
    return dataset