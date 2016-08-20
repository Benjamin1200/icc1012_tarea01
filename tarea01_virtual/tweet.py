import json
import pymongo
import tweepy

#Variables that contains the user credentials to access Twitter API
access_key = "766667542240882688-C21KB4vNzfLU88X85M3FAsYNlzxX5yi"
access_secret = "q1OAR5NFfwj80dPlx8sM9WIpo8pBny2VssS1eBJtYRxTE"
consumer_key = "xawf5vWUKQobgrUYTpSIISztG"
consumer_secret = "4eJAVF0yG5xYOBCCU407J1f6AsEisy6DtWI0f7oQmyDZMKbCVL"


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)
api = tweepy.API(auth)


class CustomStreamListener(tweepy.StreamListener):
    def __init__(self, api):
        self.api = api
        super(tweepy.StreamListener, self).__init__()

        self.db = pymongo.MongoClient().carga_twitter

    def on_data(self, tweet):
			print tweet
			self.db.icc1012.insert(json.loads(tweet))

    def on_error(self, status_code):
        return True # Don't kill the stream

    def on_timeout(self):
        return True # Don't kill the stream


sapi = tweepy.streaming.Stream(auth, CustomStreamListener(api))
sapi.filter(track=['Rio'])
