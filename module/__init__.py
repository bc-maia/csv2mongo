import os
import sys
import pandas as pd
from pymongo import MongoClient
import settings as cfg


def split(text):
    if type(text) == str:
        return text.split("|")


def strip(text):
    if type(text) == str:
        return text.strip()


def read_csv(location, columns):
    data = pd.read_csv(filepath_or_buffer=location)
    data = data.reindex(columns=columns)
    data["genres"] = data["genres"].apply(split)
    data["plot_keywords"] = data["plot_keywords"].apply(split)
    data["movie_title"] = data["movie_title"].apply(strip)
    data_dict = data.to_dict("records")
    return data_dict


def get_mongo_client(cfg):
    mongo_client = MongoClient(host=cfg.HOST, port=cfg.PORT)
    collection = mongo_client[cfg.COLLECTION]
    client = collection[cfg.DATABASE]
    return client


def csv_to_mongo(client, data):
    for movie in data:
        client.insert_one(movie)


def main():
    mongo_client = get_mongo_client(cfg)
    csv_data = read_csv("./data/movie_metadata.csv", cfg.COLUMNS_REORDERING)
    csv_to_mongo(mongo_client, csv_data)


if __name__ == "__main__":
    main()
