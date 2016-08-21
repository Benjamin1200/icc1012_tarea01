import time
import pymongo
import datetime

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


def initialize_connection():
    database = pymongo.MongoClient().tarea01
    return database

def get_tweets_per_hour(collection):
    topic = "Rio"
    results = collection.aggregate([
        {"$group": {"_id": {
            "timestamp_ms": {
                "month": { "$month": "$timestamp_ms"},
                "day": { "$dayOfMonth": "$timestamp_ms"},
                "year": { "$year": "$timestamp_ms"},
                "hour": {"$hour": "$timestamp_ms"}}
        },
            "count": { "$sum": 1}
        }
        }])

    for result in results:
        #print result
        print "In {0}/{1}/{2} at the hour {3} there were: {4} tweets, about {5}".format(int(result['_id']['timestamp_ms']['day']),
                                                                                        int(result['_id']['timestamp_ms']['month']),
                                                                                        int(result['_id']['timestamp_ms']['year']),
                                                                                        int(result['_id']['timestamp_ms']['hour']),
                                                                                        int(result['count']),
                                                                                        topic)

def get_average_of_tweets_per_hour(collection):
    topic = "Rio"
    results = collection.aggregate([
        {"$group": {"_id": {
            "timestamp_ms": {
                "month": {"$month": "$timestamp_ms"},
                "day": {"$dayOfMonth": "$timestamp_ms"},
                "year": {"$year": "$timestamp_ms"},
                "hour": {"$hour": "$timestamp_ms"}}
        },
            "count": {"$sum": 1}
        }
        }])

    amount_of_tweets = 0
    hours_seen = 0
    for result in results:
        amount_of_tweets += int(result['count'])
        hours_seen += 1
    average_of_tweets_per_hour = amount_of_tweets/hours_seen
    print "From all the data, in average there were: {0} tweets every hour, about {1}".format(average_of_tweets_per_hour,
                                                                                              topic)
def get_tweet_location_statics(collection):
    topic = "Rio"
    tweets_amount = collection.find({"place.country": {"$ne": None}}).count()
    #print tweets_amount

    results = collection.aggregate([{"$match": {"place.country": {"$ne": None}}},
                                    {"$group": {"_id": "$place.country", "count": {"$sum": 1}}}
                                    ])
    results_list = []
    for result in results:
        #print result
        results_list.append(result)
    sorted_results = sorted(results_list, key=lambda k: k["_id"])
    for result in sorted_results:
        #print result
        print "{0}% of all the tweets (with country) about {1}, were from {2}".format(
            round(float(result['count']) / tweets_amount, 4) * 100, topic, result['_id'].encode('utf-8'))

def get_average_of_followers(collection):
    # This counts the same user if it post another tweet about the topic, making the average higher or lower.
    # But is okay, since this is to know the average followers per tweet not user.
    topic = "Rio"
    amount_of_tweets = collection.find().count()
    results = collection.aggregate([{"$group": {"_id": None, "sum": {"$sum": "$user.followers_count"}}}
                                    ])
    total_followers = 0
    for result in results:
        total_followers = result["sum"]
    print "The average of followers per tweet of every user that creates a tweet about {0}, is: {1}".\
        format(topic, round(float(total_followers) / amount_of_tweets, 2))

db = initialize_connection()
collection = db.tweets

# First Metric
time_start = time.clock()
get_tweets_per_hour(collection)
time_end = time.clock()
print "Time taken to completion of the metric: {0} in processor time".format(time_end - time_start)

# Second Metric
time_start = time.clock()
get_tweet_location_statics(collection)
time_end = time.clock()
print "Time taken to completion of the metric: {0} in processor time".format(time_end - time_start)

# Third Metric
time_start = time.clock()
get_average_of_tweets_per_hour(collection)
time_end = time.clock()
print "Time taken to completion of the metric: {0} in processor time".format(time_end - time_start)

# Fourth Metric
time_start = time.clock()
get_average_of_followers(collection)
time_end = time.clock()
print "Time taken to completion of the metric: {0} in processor time".format(time_end - time_start)
