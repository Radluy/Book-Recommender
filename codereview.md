1. add type hints - better ide linting / completion, better for collaboration with other devs, less error-prone
2. extract constants to top of file, don't need to change every occurence, e.g. path for dataset, threshold for number of reviews
3. don't hardcore inputs, make the user choose and calculate based on that - correlation based on lotr part1
4. rewriting same variable with different types, first df then list
5. error_bad_lines arg of read_csv doesn't exist -> different version? changed to on_bad_lines='skip'
6. inconsistent ' / "
7. error in tab agg -> non numeric field

LoR -> LotR # STANDARD NAMING !!!

good stuff: expressive naming, nice comments, flat tree - not too many nests, 

###########################

encoding as cp1251 - weird values no matter the encoding

future 
    - multiple books/authors on input?
    - maybe store the correlation matrix somewhere? cache results?
    - pickle dataset? faster startup, flag on new data
    - api behind oauth
    - retry on download
    - rate limiting
    - store to some sort of DB
    - endpoint for data update / upload
    - pydantic schema for api response formats
    - refactor to spark
    - logging to better handle debug
    

docker build -t book-recommender .   
docker run -p 8000:8000 book-recommender  


Architecture:
ETL module - download from api / scrape
           - clean, transform
           - store cleaned as pickle
Recommendation algo - loads dataset into memory
                    - accepts input books + authors
                    - returns recommendations
API - endpoints connecting to algo
    - optional endpoints for standard data listing
Frontend - html webpage server from root endpoint
         - sends user input to recommend algo and displays result



