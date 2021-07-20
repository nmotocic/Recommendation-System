import logging
import os
from pathlib import Path
from gqlalchemy import Match, Memgraph
from functools import wraps
import time

log = logging.getLogger(__name__)

def init_log():
    logging.basicConfig(level=logging.INFO)
    log.info("Logging enabled")
    logging.getLogger("werkzeug").setLevel(logging.WARNING)

init_log()
memgraph = Memgraph()
connection_established = False
while(not connection_established):
    try:
        if(memgraph._get_cached_connection().is_active()):
            connection_established = True
    except:
        log.info("Memgraph is not running.")
        time.sleep(4)


def log_time(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        duration = time.time() - start_time
        log.info(f"Time for {func.__name__} is {duration}")
        return result
    return wrapper

@log_time
def load_data():
    """ Load book data """
    books_path = Path("/usr/lib/memgraph/import-data").joinpath("BX_Books_Summary.csv")
    users_path = Path("/usr/lib/memgraph/import-data").joinpath("BX_Users_Rated.csv")
    books_users_rating = Path("/usr/lib/memgraph/import-data").joinpath("BX_Ratings.csv")
    
    memgraph.execute_query("""CREATE INDEX ON :Book(isbn);""")
    memgraph.execute_query("""CREATE INDEX ON :User(user_id);""")

    memgraph.execute_query(f""" LOAD CSV FROM "{books_path}" WITH HEADER AS row 
                                CREATE (b: Book {{ 
                                isbn: row.isbn , 
                                title:  row.book_title, 
                                author: row.book_author, 
                                publisher: row.publisher, 
                                year_of_publishing: row.year_of_publication, 
                                language: row.Language, 
                                summary:  row.Summary, 
                                img: row.img_m 
                            }}); """)
    

    memgraph.execute_query(f""" LOAD CSV FROM "{users_path}" WITH HEADER AS row
                                CREATE (u: User {{
                                    user_id : row.user_id,
                                    age: row.age,
                                    city : row.city,
                                    state : row.state,
                                    country : row.country
                                }}); """)

    memgraph.execute_query(f""" LOAD CSV FROM "{books_users_rating}" WITH HEADER AS row
                                MATCH (u:User), (b:Book) WHERE u.user_id = row.UserID AND b.isbn = row.ISBN
                                CREATE (u)-[:RATED {{ rating: row.BookRating }}] -> (b) ;
                                """)
def main():
    memgraph.execute_query("MATCH (n) DETACH DELETE n;")
    load_data()


if __name__ == "__main__":
    main()