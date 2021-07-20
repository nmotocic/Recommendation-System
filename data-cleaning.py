import os
import pandas as pd
from pathlib import Path
import random

_here = Path(__file__).parent

def remove_non_english(frame, column):
    frame[column].replace({r'[^\x00-\x7F]+':""}, regex = True, inplace = True)
    frame.drop(frame[frame[column] == ''].index, inplace = True)
    frame.index = range(len(frame.index))
    return frame

def remove_quotation_marks(frame, column):
    frame[column].str.strip('"')
    frame.drop(frame[frame[column] == ''].index, inplace = True)
    frame.index = range(len(frame.index))
    return frame

def remove_brackets(frame, column):
    frame[column] = frame[column].str.lstrip("['")
    frame[column] = frame[column].str.rstrip("']")
    frame.drop(frame[frame[column] == ''].index, inplace = True)
    frame.index = range(len(frame.index))
    return frame

def write_to_file(frame):
    categories_path = _here.joinpath("data\\BX-Categories.csv")
    with open(categories_path, "w", newline="") as f:
        ID = 1
        f.write("ID, name" + "\n")
        for cat in frame["Category"]:
            f.write(str(ID) + "," + cat + "\n")
            ID += 1
            

data_path = _here.joinpath("data\\Preprocessed_data.csv")
ratings_path = _here.joinpath("data\\BX-Book-Ratings.csv")

#Read data
data = pd.read_csv(data_path, nrows= 50000)
ratings = pd.read_csv(ratings_path, nrows= 100000)

#Remove non english characters
data = remove_non_english(data, "book_author")
data = remove_non_english(data, "Summary")
data = remove_non_english(data, "publisher")
data = remove_non_english(data, "Category")
data = remove_non_english(data, "book_title")

#Remove brackets
data = remove_brackets(data, "Category")

#Replace blanks with Null
data.replace(" ", "Null", inplace=True)

#Remove newlines
data.replace("\n", " ", inplace=True)

#Keep Unique Categories
categories  = {col: data[col].unique() for col in ["Category"] }

#Remove unnecessary columns and duplicate rows (books)
books = data.drop(columns=["user_id", "age", "location", "img_s", "img_l" , "rating", "city", "country", "state"])
books = books.drop_duplicates("isbn", keep="first")

#Remove unnecessary columns and duplicate rows (users)
users = data.drop(columns=["isbn", "location", "img_s", "img_l" , "rating", "book_author", "Summary", "Category", "book_title", "year_of_publication", "publisher", "img_m"])
users = users.drop_duplicates("user_id", keep="first")

#Make files
books.to_csv(_here.joinpath("data\\BX_Books_Summary.csv"))
users.to_csv(_here.joinpath("data\\BX_Users_Rated.csv"))
write_to_file(categories)
ratings.to_csv(_here.joinpath("data\\BX_Ratings.csv"), index=False)

print("Successful cleaning")