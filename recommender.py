from collections.abc import Iterator
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

DATASETS_DIR = Path(__file__).parent / "datasets"
BOOKS_DATA = DATASETS_DIR / 'Books.csv'
RATINGS_DATA = DATASETS_DIR / 'Ratings.csv'
RATINGS_NUMBER_THRESHOLD = 8
PROCESSED_DATA = DATASETS_DIR / 'dataset.pkl'

INPUT_BOOK = 'the fellowship of the ring (the lord of the rings, part 1)'
INPUT_AUTHOR = 'tolkien'


def list_books(dataset: pd.DataFrame) -> Iterator[Any]:
    """Returns iterator over books in dataset."""
    return iter(dataset['Book-Title'].unique())


def list_authors(dataset: pd.DataFrame) -> Iterator[Any]:
    """Returns iterator over authors in dataset."""
    return iter(dataset['Book-Author'].unique())


def recommend(dataset: pd.DataFrame, input_book: str = INPUT_BOOK, input_author: str = INPUT_AUTHOR, num_of_results: int = 10) -> KeyError | list[dict[str, str | float]]:
    """
        Main recommendation engine. Raises exception if input book/author are not found in the data.
        Otherwise returns a list of dictionaries, each representing one recommended book.
    """
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
        result_list.append(corr_input_book.sort_values('corr', ascending=False).head(num_of_results))

        # worst 10 books
        worst_list.append(corr_input_book.sort_values('corr', ascending=False).tail(num_of_results))

    rslt = result_list[0]

    return rslt.to_dict(orient='records')


if __name__ == '__main__':
    from etl import load_dataset
    dataset = load_dataset(True)
    recommendation = recommend(dataset, INPUT_BOOK, INPUT_AUTHOR)
    print(recommendation)
