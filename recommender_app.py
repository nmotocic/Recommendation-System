import logging
import os
from pathlib import Path
from gqlalchemy import Match, Memgraph
from functools import wraps
import time
from flask import Flask, Response, render_template
from argparse import ArgumentParser
import json

log = logging.getLogger(__name__)

def init_log():
    logging.basicConfig(level=logging.INFO)
    log.info("Logging enabled")
    logging.getLogger("werkzeug").setLevel(logging.WARNING)



def parse_args():
    '''
    Parse command line arguments.
    '''
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="0.0.0.0",
                        help="Host address.")
    parser.add_argument("--port", default=5000, type=int,
                        help="App port.")
    parser.add_argument("--template-folder", default="public/template",
                        help="Path to the directory with flask templates.")
    parser.add_argument("--static-folder", default="public",
                        help="Path to the directory with flask static files.")
    parser.add_argument("--debug", default=True, action="store_true",
                        help="Run web server in debug mode.")
    parser.add_argument("--load-data", default=False, action='store_true',
                        help="Load BitClout network into Memgraph.")
    print(__doc__)
    return parser.parse_args()

args = parse_args()


app = Flask (__name__, 
            template_folder=args.template_folder,
            static_folder=args.static_folder,
            static_url_path="")


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
                                    user_id : ToInteger(row.user_id),
                                    age: row.age,
                                    city : row.city,
                                    state : row.state,
                                    country : row.country
                                }}); """)

    memgraph.execute_query(f""" LOAD CSV FROM "{books_users_rating}" WITH HEADER AS row
                                MATCH (u:User), (b:Book) WHERE u.user_id = ToInteger(row.UserID) AND b.isbn = row.ISBN
                                CREATE (u)-[:RATED {{ rating: ToInteger(row.BookRating) }}] -> (b) ;
                                """)

@log_time
def recommend_cf():
    """ Find recommended books via Collaborative Filtering method"""
    
    #Find User's Similar Ratings to Others
    similar_ratings = memgraph.execute_and_fetch("""
        MATCH (u1: User {user_id: 11676}) -[r1:RATED]-> (b: Book) <-[r2:RATED] -(u2:User)
        RETURN u1.user_id, u2.user_id, b.title, r1.rating, r2.rating,
        CASE WHEN r1.rating - r2.rating < 0 THEN -(r1.rating - r2.rating) ELSE r1.rating - r2.rating END AS difference
        ORDER BY difference ASC
        LIMIT 15
    """)

    #Create Similarity score between two Users
    memgraph.execute_query("""
        MATCH (u1: User) -[r1:RATED]-> (b: Book) <-[r2:RATED]- (u2:User)
        WITH u1, u2, SUM(r1.rating * r2.rating) AS dot_product,
        SQRT (REDUCE (x=0.0, a IN COLLECT (r1.rating) | x + a*a) ) as r1_length,
        SQRT (REDUCE (y=0.0, b IN COLLECT(r2.rating) | y + b*b) ) as r2_length
        MERGE (u1)-[s:SIMILARITY]-(u2)
        SET s.similarity = dot_product/ (r1_length * r2_length)
    """)

    #Let's recommend something!
    recommedations = memgraph.execute_and_fetch("""
        MATCH (me:User)-[:SIMILARITY]->(u:User)-[r:RATED]->(b:Book)
        MATCH (me)-[:RATED]->(b2:Book)
        WITH me, b, u, r, COLLECT(b2) AS rated
        WHERE me.user_id = 11676 AND NOT b IN rated
        WITH b, COLLECT (r.rating)[0..1] AS ratings, COLLECT(u.user_id)[0..1] AS users
        WITH b, users, REDUCE(s=0, i IN ratings | s+i) / SIZE(ratings) AS recommendation
        ORDER BY recommendation DESC
        RETURN b.title, recommendation
        LIMIT 10
    """)



@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")

@log_time
@app.rounte("/load-all", methods=["GET"])
def load_all():
    """ Load graph from database"""
    try:
        results = (
                Match()
                    .node("User", variable="from")
                    .to("RATED")
                    .node("Book", variable="to")
                    .execute() 
                )

        #Check if we added the node or edge already
        nodes_set = set()
        link_set = set()
        for result in results:
            source_id = result["from"].properties["user_id"]
            source_age = result["from"].properties["age"]
            source_country = result["from"].properties["country"]

            target_id = result["to"].properties["isbn"]
            target_title = result["to"].properties["title"]
            target_author = result["to"].properties["author"]
            target_year = result["to"].properties["year_of_publishing"]
            target_language = result["to"].properties["language"]
            target_summary = result["to"].properties["summary"]

            nodes_set.add((source_id, source_age, source_country))
            nodes_set.add((target_id, target_title, target_author, target_year, target_language, target_summary))

            if (source_id, target_id) not in link_set and (target_id, source_id) not in link_set:
                link_set.add((source_id, target_id))

            nodes = [ {"id": user_id, "age": age, "country": country} for (user_id, age, country) in nodes_set]
            nodes.append( [ { "id": target_id, "title": target_title, "author": target_author, "year": target_year, "language": target_language, "summary" : target_summary }
                for (target_id, target_title, target_author, target_year, target_language, target_summary) in nodes_set ])
            links = [ {"source": s_id, "target": t_id} for (s_id, t_id) in link_set]

            response = {"nodes": nodes, "links": links}

            return Response(json.dumps(response), status=200, mimetype="application/json")

    except Exception:
        log.info("Fetching the graph was unsuccessfull. Something went wrong.")
        return ("", 500)

#TODO: Properties

def main():
    memgraph.execute_query("MATCH (n) DETACH DELETE n;")
    load_data()
    app.run(host=args.host,
            port=args.port,
            debug=args.debug)




if __name__ == "__main__":
    main()