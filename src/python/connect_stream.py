#!/Users/iiyakhruschev/opt/anaconda3/bin python
# coding: utf-8

import pandas as pd
import os
import requests
import json
from pymongo import MongoClient

consumer_key = os.getenv("CONSUMER_KEY")
consumer_secret = os.getenv("CONSUMER_SECRET")
access_token = os.getenv("ACCESS_TOKEN")
access_token_secret = os.getenv("ACCESS_TOKEN_SECRET")
bearer_token = os.getenv("BEARER_TOKEN")

client_db =  'stream'
client_col = 'frenchgp' #args.collection

# source and target connections
client = MongoClient('mongodb://localhost:27017')

# database and collection names
db = client[client_db]
col = db[client_col]

# # Build Stream

rules = [
    {"value": '((gasly OR hamilton OR verstappen OR perez OR norris OR leclerc \
OR bottas OR sainz OR gasly OR vettel OR ricciardo OR alonso OR ocon OR \
stroll OR tsunoda OR raikkonen OR giovinazzi OR schumacher OR russell \
OR mazepin OR latifi) (f1 OR formula1 OR "french grand prix" OR ricard OR \
"french GP" OR "grand prix" OR "formula 1" OR frenchgp)) -indycar \
-"indy car" -is:retweet -is:reply -is:quote'}]

rules2 = [
    {"value": '(formula1 OR #f1 OR "formula one" OR "formula 1") -indycar \
-"indy car" -is:retweet -is:reply -is:quote'}]

params = {
    'tweet.fields': 'created_at,public_metrics',
    'expansions': 'author_id,referenced_tweets.id,geo.place_id',
    'user.fields': 'created_at,location'
}


def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


headers = create_headers(bearer_token)


def getRules():
    response2 = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        headers=create_headers(bearer_token))
    print("\nGETTING RULES: ", response2.text)
    return response2.json()


def deleteRules(response2):

    response2 = getRules()
    ids = [i['id'] for i in response2['data']]
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        headers=headers,
        json=payload)
    print("\nDELETING RULES\n")


def setRules(rules):
    payload = {"add": rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        headers=create_headers(bearer_token),
        json=payload,
    )
    print("\nSET RULES: ", response.text)


def getStream(headers):
    with requests.get("https://api.twitter.com/2/tweets/search/stream",
                      headers=headers,
                      params=params,
                      stream=True) as response:
        print(response)
        for response_line in response.iter_lines():
            if response_line:
                json_response = json.loads(response_line)
                print(json.dumps(json_response, indent=4, sort_keys=True))
                col.insert_one(json.loads(response_line))
                
            

def main():
    headers = create_headers(bearer_token)
    response2 = getRules()
    deleteRules(response2)
    setRules(rules)
    setRules(rules2)
    getRules()
    while True:
        getStream(headers)
        # timeout += 1


if __name__ == "__main__":
    main()




