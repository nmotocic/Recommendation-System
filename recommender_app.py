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
    importQueriesPath = Path("/usr/lib/memgraph/import-data").joinpath("queries.cypherl")
    with open(importQueriesPath, "r", newline="") as f:
        lines = f.readlines()
        for line in lines:
            memgraph.execute_query(line)
    
def main():
    memgraph.execute_query("MATCH (n) DETACH DELETE n;")
    load_data()


if __name__ == "__main__":
    main()