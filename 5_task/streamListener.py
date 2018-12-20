# -*- coding: utf-8 -*-

import tweepy
from tweepy.streaming import json
from kafka import KafkaProducer


producer = KafkaProducer(bootstrap_servers="localhost:9092")
topic_name = "retweets-kafka"

watched_users = ["285532415", "147964447", "34200559", "338960856", 
    "200036850", "72525490", "20510157", "99918629", "82299300", "1070777285924569089"]

class MyStreamListener(tweepy.StreamListener):
    
    def on_data(self, raw_data):

        data = json.loads(raw_data)

        retweeted_status = data.get('retweeted_status')
        if retweeted_status is not None:
            author = retweeted_status.get('user')
            if author is None:
                return

            author_name = author.get('name')
            tweet_id = retweeted_status.get('id')
            text = retweeted_status.get('text')

            if author_name is None or tweet_id is None or text is None:
                return

            result = dict()
            result['author_name'] = author_name
            result['tweet_id'] = tweet_id
            result['text'] = text

            print(result)

            producer.send(topic_name, str(result).encode("utf-8"))

    def on_error(self, status):
        print(status)

consumer_token = "kek"
consumer_secret = "kek"
access_token = "kek"
access_secret = "kek"

auth = tweepy.OAuthHandler(consumer_token, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)

myStreamListener = MyStreamListener()

myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)

myStream.filter(follow=watched_users)
