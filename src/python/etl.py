#!/Users/iiyakhruschev/opt/anaconda3/bin python
# coding: utf-8

import pandas as pd
import os
import translators as ts
from datetime import datetime
from langdetect import detect
from json_parsers import *
from sqlalchemy import create_engine
from pymongo import MongoClient

# logging
logging.basicConfig(filename='logs/etl_{}'.format(datetime.now()), level=logging.INFO)
# Mongo database and collection
client_db = 'stream'
client_col = 'frenchgp'  # args.collectionc

# source and target connections
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI)

# database and collection names
db = client[client_db]
col = db[client_col]

# IMPORT
df_in = pd.DataFrame(list(col.find({})))
df_in = df_in.drop(['matching_rules'], 1)
logging.info('{} - {} records retrieved from mongo'.format(datetime.now(), len(df_in)))


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ DEFINE FUNCTIONS ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def getData(df):
    """
    Convert the nexted json inside the data column to its own dataframe
    """
    df['author_id'] = df.apply(
        lambda df: parse_json(df, 'data', 'author_id'), 1)
    df['created_at'] = df.apply(
        lambda df: parse_json(df, 'data', 'created_at'), 1)
    df['geo'] = df.apply(lambda df: parse_json(df, 'data', 'geo'), 1)
    df['tweet_id'] = df.apply(lambda df: parse_json_exact(df, 'data', 'id'), 1)
    df['raw_text'] = df.apply(lambda df: parse_json(df, 'data', 'text'), 1)
    df = df[['tweet_id', 'author_id', 'created_at', 'raw_text']]
    return df


def getTranslation(df):
    clean_text = df['clean_text']
    try:
        translated_text = ts.google(clean_text, if_use_cn_host=True)
    except Exception:
        translated_text = clean_text
    return translated_text


def getCleanText(df):
    clean_text = ''.join(e for e in df['raw_text'] if e.isascii())
    clean_text = ''.join(e for e in clean_text if e not in ["!", "@", "#"])
    return clean_text


def getLanguage(df):
    clean_text = ''.join(e for e in df['raw_text'] if e.isascii())
    clean_text = ''.join(e for e in clean_text if e not in ["!", "@", "#"])
    try:
        language = detect(clean_text)
    except Exception:
        language = ''
    return language


def getUsers(df):
    users = df['includes']['users']
    return users


def getUserDataframe(df):
    df['user_created_at'] = df.apply(lambda df: parse_json(df, 'users', 'created_at'), 1)
    df['user_id'] = df.apply(lambda df: parse_json(df, 'users', 'id'), 1)
    df['location'] = df.apply(lambda df: parse_json(df, 'users', 'location'), 1)
    df['name'] = df.apply(lambda df: parse_json_exact(df, 'users', 'name'), 1)
    df['username'] = df.apply(lambda df: parse_json(df, 'users', 'username'), 1)
    df = df.drop(['users'], 1)
    return df


def escapeArray(df, column):
    if len(df[column]) == 0:
        return ''
    else:
        return df[column]

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ CLEAN TWEET DATA / TRANSLATE ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

data_df = getData(df_in)
data_df['clean_text'] = data_df.apply(lambda data_df: getCleanText(data_df), 1)
logging.info('{} - text cleaned'.format(datetime.now()))

data_df['language'] = data_df.apply(lambda data_df: getLanguage(data_df), 1)
logging.info('{} - languages identified '.format(datetime.now()))

data_df_translate = data_df[data_df['language'] != 'en'][['tweet_id', 'clean_text']]
logging.info('{} - {} non-english language records detected'.format(datetime.now(), len(data_df_translate)))
data_df_translate['translated_text'] = data_df_translate.apply(lambda data_df_translate: getTranslation(data_df_translate), 1)
logging.info('{} - {} non-english language records translated'.format(datetime.now(), len(data_df_translate)))

data_df = data_df \
    .merge(data_df_translate, on='tweet_id', how='left') \
    .rename(index=str, columns={'clean_text_x': 'clean_text'}) \
    .drop('clean_text_y', 1)

data_df['translated_text'] = data_df['translated_text'].fillna(data_df['clean_text'])
data_df = data_df[['tweet_id', 'author_id', 'created_at',
                   'language', 'raw_text', 'clean_text', 'translated_text']]


# SEND TO POSTGRES
logging.info('{} - sending tweet table to postgres'.format(datetime.now()))
engine = create_engine('postgresql://postgres@localhost:5432/frenchgp')
data_df.to_sql('tweet', engine)
logging.info('{} - export successful'.format(datetime.now()))

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ USERS MENTIONED ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

df_in['users'] = df_in.apply(lambda df_in: getUsers(df_in), 1)
users_exploded = df_in[['tweet_id', 'users']].explode('users')
users_df = getUserDataframe(users_exploded)
users_df['location'] = users_df.apply(
    lambda users_df: escapeArray(users_df, 'location'), 1)


# SEND TO POSTGRES
logging.info('{} - sending users mentioned table to postgres'.format(datetime.now()))
users_df.to_sql('users_mentioned', engine)
logging.info('{} - export successful'.format(datetime.now()))
logging.info('{} - ETL COMPLETE'.format(datetime.now()))

