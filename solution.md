## Code review
1. add type hints - better ide linting / completion, better for collaboration with other devs, less error-prone
2. extract constants to top of file, don't need to change every occurence, e.g. path for dataset, threshold for number of reviews
3. don't hardcore inputs, make the user choose and calculate based on that - correlation based on lotr part1
4. rewriting same variable with different types, first df then list
5. error_bad_lines arg of read_csv doesn't exist -> different version? changed to on_bad_lines='skip'
6. inconsistent ' / "
7. error in tab agg -> non numeric field

(LoR -> LotR # STANDARD NAMING !!!)

encoding as cp1251 - weird values no matter the encoding


good stuff: expressive naming, nice comments, flat tree - not too many nests

## Architecture

### ETL module 
- download from kaggle api / scrape sharepoint with selenium
- clean, transform: empty and incorrect values, useless columns
- store cleaned data as pickle: faster loadup

### Recommendation algo
- loads dataset into memory
- accepts input books + authors
- returns recommendations

### API 
- endpoints connecting to algo
- optional endpoints for standard data listing
  
### Frontend
- html webpage server from root endpoint
- sends user input to recommend algo and displays result


## Libraries used
- kaggle, selenium (download)
- fastapi (api endpoints)
- pandas (data handling)

## Possible future improvements
- api behind oauth
- retry on download
- rate limiting on api
- store processed to DB
- internal endpoint for running ETL process
- pydantic schema for api response formats
- refactor to spark
- logging to better handle debug
- endpoint for worst books from engine
- timestamps for data download, data process etc.
- tests to check api is working
- real configfile
- specific kaggle account for this image
- multiple books/authors on input
- maybe store the correlation matrix somewhere - cache results