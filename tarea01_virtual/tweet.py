import json
import pymongo
import tweepy
import psycopg2

#Variables that contains the user credentials to access Twitter API
access_key = "766667542240882688-C21KB4vNzfLU88X85M3FAsYNlzxX5yi"
access_secret = "q1OAR5NFfwj80dPlx8sM9WIpo8pBny2VssS1eBJtYRxTE"
consumer_key = "xawf5vWUKQobgrUYTpSIISztG"
consumer_secret = "4eJAVF0yG5xYOBCCU407J1f6AsEisy6DtWI0f7oQmyDZMKbCVL"


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)
api = tweepy.API(auth)

# The table schema: CREATE TABLE tweets (id SERIAL PRIMARY KEY, tweet_id BIGINT NOT NULL, created_at VARCHAR NOT NULL, timestamp_ms TIMESTAMP, text VARCHAR NOT NULL, place_name VARCHAR, place_country VARCHAR, user_screen_name VARCHAR NOT NULL, user_id BIGINT NOT NULL, user_location VARCHAR, user_followers_count INTEGER NOT NULL, user_friends_count INTEGER NOT NULL )

#{"created_at":"Sat Aug 20 20:58:47 +0000 2016","id":767103611033780229, "text":"In the news: Australian athletes in Rio released by police after agreeing to fine https:\/\/t.co\/HCzKuEngb0", "in_reply_to_status_id":null,"in_reply_to_user_id":null,"in_reply_to_screen_name":null,
# "user":{"id":1954280094,"id_str":"1954280094","name":"The Bangor Insider","screen_name":"BangorInsider","location":"Bangor, Maine","url":"http:\/\/www.bangorinsider.com","description":"The Bangor Region's Online News Portal. Promoting Our Local Media.","protected":false,"verified":false,"followers_count":359,"friends_count":45,"listed_count":37,"favourites_count":2,"statuses_count":136150,"created_at":"Fri Oct 11 14:31:08 +0000 2013","utc_offset":null,"time_zone":null,"geo_enabled":false,"lang":"en","contributors_enabled":false,"default_profile":true,"default_profile_image":false,"following":null,"follow_request_sent":null,"notifications":null},
# "geo":null,"coordinates":null,"place":null,"contributors":null,"is_quote_status":false,"retweet_count":0,"favorite_count":0,
# "entities":{"hashtags":[],
#   "urls":[{"url":"https:\/\/t.co\/HCzKuEngb0","expanded_url":"http:\/\/bangorinsider.com\/australian-athletes-in-rio-released-by-police-after-agreeing-to-fine\/","display_url":"bangorinsider.com\/australian-ath\u2026","indices":[82,105]}],
#   "user_mentions":[],"symbols":[]},
# "favorited":false,"retweeted":false,"possibly_sensitive":false,"filter_level":"low","lang":"en","timestamp_ms":"1471726727623"}

# "geo_enabled":true,"lang":"pt",
# "geo":{"type":"Point","coordinates":[-22.75719216,-43.45593728]},
# "coordinates":{"type":"Point","coordinates":[-43.45593728,-22.75719216]},
# "place":{"id":"4029837e46e8e369","url":"https:\/\/api.twitter.com\/1.1\/geo\/id\/4029837e46e8e369.json","place_type":"city","name":"Nova Igua\u00e7u","full_name":"Nova Igua\u00e7u, Brasil","country_code":"BR","country":"Brasil",
#   "bounding_box":{"type":"Polygon","coordinates":[[[-43.681932,-22.865838],[-43.681932,-22.527218],[-43.366801,-22.527218],[-43.366801,-22.865838]]]},
#   "attributes":{}},

class CustomStreamListener(tweepy.StreamListener):
    def __init__(self, api):
        self.api = api
        super(tweepy.StreamListener, self).__init__()

        self.db = pymongo.MongoClient().tarea01

        # Postgresql initialization
        self.connection = psycopg2.connect("dbname=tarea01 user=postgres password=postgres host=localhost")
        self.cursor = self.connection.cursor()

    def on_data(self, tweet):
        #print tweet
        values = json.loads(tweet)
        if 'place' not in values:
            values['place'] = None
        if values['place'] is None:
            values['place'] = {"name": None, "country": None}
        # print values['id'], values['created_at'], values['timestamp_ms'], values['text'], values['place']['name'], values['place']['country'], values['user']['screen_name'], values['user']['id'], values['user']['location'], values['user']['followers_count'], values['user']['friends_count']
        try:
            self.cursor.execute(
                "INSERT INTO tweets (tweet_id, created_at, timestamp_ms, text, place_name, place_country, user_screen_name, user_id, user_location, user_followers_count, user_friends_count) VALUES (%s, %s, TO_TIMESTAMP(%s::double precision / 1000), %s, %s, %s, %s, %s, %s, %s, %s);",
                (values['id'], values['created_at'], values['timestamp_ms'], values['text'], values['place']['name'],
                 values['place']['country'], values['user']['screen_name'], values['user']['id'],
                 values['user']['location'], values['user']['followers_count'], values['user']['friends_count']))
            self.connection.commit()
            self.db.tweets.insert(json.loads(tweet))
        except:
            pass

    def on_error(self, status_code):
        return True # Don't kill the stream

    def on_timeout(self):
        return True # Don't kill the stream

    # always use this step to begin clean
    def reset_cursor(self):
        self.cursor = self.connection.cursor()

sapi = tweepy.streaming.Stream(auth, CustomStreamListener(api))
sapi.filter(track=['Rio'])
