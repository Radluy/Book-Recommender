from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse

from recommender import list_authors, list_books, recommend
from etl import load_dataset

app = FastAPI()
app.state.dataset = load_dataset()  # keep one copy in memory - readOnly
app.state.index_html = (Path(__file__).parent / 'index.html').read_text()


def paginate(items: list[Any], page: int, page_size: int):
    start = (page - 1) * page_size
    end = start + page_size
    return {
        "page": page,
        "page_size": page_size,
        "total": len(items),
        "items": items[start:end]
    }

@app.get("/recommend", response_class=JSONResponse)
def recommend_books(entry_book: str, entry_author: str, num_of_results: int = 10):
    entry_book = entry_book.lower()
    entry_author = entry_author.lower()
    try:
        result = recommend(app.state.dataset, entry_book, entry_author, num_of_results)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

    return result
    
@app.get("/books")
def list_books_api(page: int = Query(1, ge=1), page_size: int = Query(10, ge=1, le=50)):  # pyright: ignore[reportCallInDefaultInitializer]
    items = list(list_books(app.state.dataset))
    return paginate(items, page, page_size)

@app.get("/authors")
def list_authors_api(page: int = Query(1, ge=1), page_size: int = Query(10, ge=1, le=50)):  # pyright: ignore[reportCallInDefaultInitializer]
    items = list(list_authors(app.state.dataset))
    return paginate(items, page, page_size)


@app.get("/", response_class=HTMLResponse)
def root():
    return app.state.index_html
