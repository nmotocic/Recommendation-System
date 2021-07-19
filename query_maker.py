import os
import pandas as pd
from pathlib import Path
import itertools
import shutil

_here = Path(__file__).parent

data_path = _here.joinpath("data\\Preprocessed_data_cleaned.csv")

data = pd.read_csv(data_path)

#Find uniques
books = {col: data[col].unique() for col in ["isbn"] }
users = {col: data[col].unique() for col in ["user_id"] }
categories  = {col: data[col].unique() for col in ["Category"] }

#Create queries and put them in the file
importQueriesPath = _here.joinpath("queries\\queries.cypherl")

book_list = [] #keep track of all the books in the dataset
user_list = [] #keep track of all the users in the dataset

with open(importQueriesPath, "w", newline="") as f:
    f.write("CREATE INDEX ON :Book (isbn);")
    f.write("\n")
    f.write("CREATE INDEX ON :Person (user_id);")
    f.write("\n")


    for index, row in data.iterrows():
        if(row["isbn"] not in book_list):
            f.write(f'CREATE (b: Book {{ isbn: "{ row["isbn"] }", title: "{ row["book_title"]}", author: "{row["book_author"]}", publisher: "{row["publisher"]}", year_of_publishing: "{row["year_of_publication"]}", language: "{row["Language"]}", summary: "{ row["Summary"]}", img: "{row["img_m"]}"  }} ); \n')
            book_list.append(row["isbn"])
        
    for index, row in data.iterrows():    
        if(row["user_id"] not in user_list):
            f.write(f'CREATE (u: User {{ id: "{row["user_id"] }", age: "{row["age"]}", city: "{row["city"]}", country: "{row["country"]}", state: "{row["state"]}" }} ); \n')
            user_list.append(row["user_id"])

    for cat in categories["Category"]:
        f.write(f'CREATE (c: Category {{ name: "{ cat}" }} ); ')
        f.write("\n")
    
    for index,row in data.iterrows():
        for user in users["user_id"]:
            if (user == row['user_id']):
                f.write(f'MATCH (from: User), (to:Book) WHERE from.id = "{user}" AND to.isbn = "{row["isbn"]}" CREATE (from)-[:RATED {{ rating: {row["rating"]} }}]->(to); \n')
    
    book_lst = []
    for index,row in data.iterrows():
        if(row["isbn"] not in book_lst):
            f.write(f'MATCH (from: Book), (to: Category) WHERE from.isbn = "{row["isbn"]}" AND to.name = "{row["Category"]}" CREATE (from)-[:IS_IN_CATEGORY]->(to); \n')
            book_lst.append(row["isbn"])


originalPath = importQueriesPath
targetPath = _here.joinpath("memgraph\\import-data\\queries.cypherl")

shutil.copyfile(originalPath, targetPath)

print("Successful query making")