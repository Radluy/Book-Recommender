from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from book_rec import recommend, list_books, list_authors, load_dataset
from pathlib import Path

app = FastAPI()
dataset = load_dataset()  # keep one copy in memory - readOnly, optionally save to app.state
index_html = (Path(__file__).parent / 'index.html').read_text()

@app.get("/book_recommend")
def recommend_books(entry_book: str, entry_author: str, num_of_results: int = 10):  # pyright: ignore[reportUnknownParameterType]
    entry_book = entry_book.lower()
    entry_author = entry_author.lower()
    try:
        result = recommend(dataset, entry_book, entry_author)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    return result[0:num_of_results]   # pyright: ignore[reportIndexIssue, reportUnknownVariableType]
    
@app.get("/books")
def list_books_api(page: int = Query(1, ge=1), page_size: int = Query(10, ge=1, le=50)):  # pyright: ignore[reportCallInDefaultInitializer]
    items = list(list_books(dataset))
    start = (page - 1) * page_size
    end = start + page_size
    paginated_items = items[start:end]
    
    return {
        "page": page,
        "page_size": page_size,
        "total": len(items),
        "items": paginated_items,
    }

@app.get("/authors")
def list_authors_api(page: int = Query(1, ge=1), page_size: int = Query(10, ge=1, le=50)):  # pyright: ignore[reportCallInDefaultInitializer]
    items = list(list_authors(dataset))
    start = (page - 1) * page_size
    end = start + page_size
    paginated_items = items[start:end]
    
    return {
        "page": page,
        "page_size": page_size,
        "total": len(items),
        "items": paginated_items,
    }


@app.get("/", response_class=HTMLResponse)
def root():
    return index_html
