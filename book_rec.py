from collections.abc import Iterator
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from download import download_data

DATASETS_DIR = Path(__file__).parent / "datasets"
BOOKS_DATA = DATASETS_DIR / 'Books.csv'
RATINGS_DATA = DATASETS_DIR / 'Ratings.csv'
RATINGS_NUMBER_THRESHOLD = 8
PROCESSED_DATA = DATASETS_DIR / 'dataset.pkl'

INPUT_BOOK = 'the fellowship of the ring (the lord of the rings, part 1)'
INPUT_AUTHOR = 'tolkien'


def validate_isbn(isbn) -> bool:
    """Validates that isbn is in the correct format"""
    sum = 0
    for idx, char in enumerate(isbn):
        if char == 'X':
            char = 10
        sum += int(char) * (10 - idx)
    return sum % 11 == 0

def fix_encoding(s):
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
    ratings: pd.DataFrame = pd.read_csv(RATINGS_DATA, encoding='utf-8', sep=',')
    ratings = clean_ratings_data(ratings)

    # load books
    books: pd.DataFrame = pd.read_csv(BOOKS_DATA, encoding='utf-8', sep=',', on_bad_lines='skip')
    books = clean_books_data(books)


    dataset: pd.DataFrame = pd.merge(ratings, books, on=['ISBN'])

    dataset.to_pickle(PROCESSED_DATA)
    return dataset


def list_books(dataset: pd.DataFrame) -> Iterator[Any]:
    return iter(dataset['Book-Title'].unique())


def list_authors(dataset: pd.DataFrame) -> Iterator[Any]:
    return iter(dataset['Book-Author'].unique())


def recommend(dataset: pd.DataFrame, input_book: str = INPUT_BOOK, input_author: str = INPUT_AUTHOR) -> KeyError | list[dict[str, str | float]]:
    if not dataset["Book-Title"].str.contains(input_book, case=False, na=False, regex=False).any():
        raise KeyError(f'Book {input_book} not found in the dataset')
    if not dataset["Book-Author"].str.contains(input_author, case=False, na=False, regex=False).any():
        raise KeyError(f'Author {input_author} not found in the dataset')

    similar_readers_ser: pd.Series = dataset['User-ID'][(dataset['Book-Title'].str.contains(
        input_book, case=False, na=False, regex=False)) & (dataset['Book-Author'].str.contains(input_author, case=False, na=False, regex=False))]
    similar_readers_lst = similar_readers_ser.tolist()
    similar_readers = np.unique(similar_readers_lst)

    # final dataset
    books_of_similar_readers: pd.DataFrame = dataset[(dataset['User-ID'].isin(similar_readers))]
    # Number of ratings per other books in dataset
    number_of_rating_per_book: pd.DataFrame = books_of_similar_readers.groupby(
        ['Book-Title']).agg('count').reset_index()

    # select only books which have actually higher number of ratings than threshold
    books_to_compare_ser: pd.Series = number_of_rating_per_book['Book-Title'][number_of_rating_per_book['User-ID']
                                                                              >= RATINGS_NUMBER_THRESHOLD]
    books_to_compare = books_to_compare_ser.tolist()

    ratings_data_raw: pd.DataFrame = books_of_similar_readers[['User-ID', 'Book-Rating', 'Book-Title']][
        books_of_similar_readers['Book-Title'].isin(books_to_compare)
    ]

    # group by User and Book and compute mean
    # reset index to see User-ID in every row
    ratings_data_mean: pd.DataFrame = ratings_data_raw.groupby(['User-ID', 'Book-Title'])['Book-Rating'] \
        .mean() \
        .to_frame() \
        .reset_index()

    dataset_for_corr = ratings_data_mean.pivot(index='User-ID', columns='Book-Title', values='Book-Rating')

    input_book_list = [input_book]

    result_list = []
    worst_list = []
    # for each input book compute:
    for input_book in input_book_list:

        # Take out the selected book from correlation dataframe
        dataset_of_other_books = dataset_for_corr.copy(deep=False)
        dataset_of_other_books.drop([input_book], axis=1, inplace=True, errors='ignore')

        # empty lists
        book_titles = []
        correlations = []
        avgrating = []

        # corr computation
        for book_title in list(dataset_of_other_books.columns.values):
            book_titles.append(book_title)

            if input_book not in dataset_for_corr:
                raise KeyError(f"No matching books found for {input_book}")
            correlations.append(dataset_for_corr[input_book].corr(dataset_of_other_books[book_title]))

            tab = (ratings_data_raw[ratings_data_raw['Book-Title'] == book_title]
                   .groupby(ratings_data_raw['Book-Title']).mean(numeric_only=True))

            avgrating.append(tab['Book-Rating'].min())
        # final dataframe of all correlation of each book
        corr_input_book = pd.DataFrame(list(zip(book_titles, correlations, avgrating)),
                                       columns=['book', 'corr', 'avg_rating'])
        # corr_input_book.head()
        corr_input_book = corr_input_book.dropna()
        # top 10 books with highest corr
        result_list.append(corr_input_book.sort_values('corr', ascending=False).head(10))

        # worst 10 books
        worst_list.append(corr_input_book.sort_values('corr', ascending=False).tail(10))

    # print("Correlation for book:", input_book_list[0])
    # print("Average rating of LOR:", ratings_data_raw[ratings_data_raw['Book-Title']=='the fellowship of the ring (the lord of the rings, part 1'].groupby(ratings_data_raw['Book-Title']).mean()))
    rslt = result_list[0]
    # print(rslt)
    # result = [dict(a=x, b=y, c=z) for book, corr, rating in zip(book_title, list2, list3)]
    return rslt.to_dict(orient='records')


if __name__ == '__main__':
    dataset = load_dataset(True)
    recommend(dataset)
