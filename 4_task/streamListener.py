# -*- coding: utf-8 -*-

import tweepy
from tweepy.streaming import json
from kafka import KafkaProducer


producer = KafkaProducer(bootstrap_servers="localhost:9092")
topic_name = "tweets-kafka"

watched_users = ["285532415", "147964447", "34200559", "338960856", 
    "200036850", "72525490", "20510157", "99918629", "82299300", 
    "811377", "25073877", "1070777285924569089"]

class MyStreamListener(tweepy.StreamListener):
    
    def on_data(self, raw_data):

        data = json.loads(raw_data)

        result = dict()

        user =  data.get('user')
        if user is None:
            return

        user_id = user.get('id')
        user_name = user.get('screen_name')

        if user_id is not None and user_name is not None:
            if str(user_id) in watched_users:
                result['id'] = user_id
                result['name'] = user_name
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
