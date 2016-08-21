import time
import psycopg2

# The table schema: CREATE TABLE tweets (id SERIAL PRIMARY KEY, tweet_id BIGINT NOT NULL, created_at VARCHAR NOT NULL, timestamp_ms TIMESTAMP, text VARCHAR NOT NULL, coordinates VARCHAR, place_name VARCHAR, place_country VARCHAR, user_screen_name VARCHAR NOT NULL, user_id BIGINT NOT NULL, user_location VARCHAR, user_followers_count INTEGER NOT NULL, user_friends_count INTEGER NOT NULL )

def initialize_connection():
    # Postgresql initialization
    connection = psycopg2.connect("dbname=tarea01 user=postgres password=postgres host=localhost")
    return connection

def close_connection(connection):
    connection.close()

def get_tweets_per_hour(cursor):
    topic = "Rio"
    cursor.execute("SELECT COUNT(*), EXTRACT(YEAR FROM TIMESTAMP_MS) AS YEAR, "
                   "EXTRACT(MONTH FROM TIMESTAMP_MS) AS MONTH, EXTRACT(DAY FROM TIMESTAMP_MS) AS DAY, "
                   "EXTRACT(HOUR FROM TIMESTAMP_MS) AS HOUR "
                   "FROM TWEETS "
                   "GROUP BY YEAR, MONTH, DAY, HOUR "
                   "ORDER BY YEAR, MONTH, DAY, HOUR;")
    tweets_timestamps = cursor.fetchall()

    for tweets_every_hour in tweets_timestamps:
        print "In {0}/{1}/{2} at the hour {3} there were: {4} tweets, about {5}".format(int(tweets_every_hour[3]),
                                                                                        int(tweets_every_hour[2]),
                                                                                        int(tweets_every_hour[1]),
                                                                                        int(tweets_every_hour[4]),
                                                                                        int(tweets_every_hour[0]),
                                                                                        topic)

def get_average_of_tweets_per_hour(cursor):
    topic = "Rio"
    cursor.execute("SELECT COUNT(*), EXTRACT(YEAR FROM TIMESTAMP_MS) AS YEAR, "
                   "EXTRACT(MONTH FROM TIMESTAMP_MS) AS MONTH, EXTRACT(DAY FROM TIMESTAMP_MS) AS DAY, "
                   "EXTRACT(HOUR FROM TIMESTAMP_MS) AS HOUR "
                   "FROM TWEETS "
                   "GROUP BY YEAR, MONTH, DAY, HOUR "
                   "ORDER BY YEAR, MONTH, DAY, HOUR;")
    tweets_timestamps = cursor.fetchall()

    amount_of_tweets = 0
    hours_seen = 0
    for tweets_every_hour in tweets_timestamps:
        amount_of_tweets += int(tweets_every_hour[0])
        hours_seen += 1
    average_of_tweets_per_hour = amount_of_tweets/hours_seen
    print "From all the data, in average there were: {0} tweets every hour, about {1}".format(average_of_tweets_per_hour,
                                                                                              topic)
def get_tweet_location_statics(cursor):
    topic = "Rio"
    cursor.execute("SELECT COUNT(*) FROM TWEETS WHERE PLACE_COUNTRY IS NOT NULL;")
    tweets_amount = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*), PLACE_COUNTRY "
                   "FROM TWEETS WHERE PLACE_COUNTRY IS NOT NULL "
                   "GROUP BY PLACE_COUNTRY ORDER BY PLACE_COUNTRY;")
    tweets_locations = cursor.fetchall()
    for tweets_location in tweets_locations:
        print "{0}% of all the tweets (with country) about {1}, were from {2}".format(round(float(tweets_location[0])/tweets_amount, 4)*100, topic, tweets_location[1])

def get_average_of_followers(cursor):
    topic = "Rio"
    cursor.execute("SELECT COUNT(*) FROM TWEETS;")
    amount_of_tweets = cursor.fetchone()[0]
    cursor.execute("SELECT SUM(USER_FOLLOWERS_COUNT) FROM TWEETS;")
    total_followers = cursor.fetchone()[0]
    print "The average of followers of every user that creates a tweet about {0}, is: {1}".format(topic, round(float(total_followers)/amount_of_tweets, 2) )

connection = initialize_connection()

# First Metric
cursor = connection.cursor()
time_start = time.clock()
get_tweets_per_hour(cursor)
time_end = time.clock()
cursor.close()
print "Time taken to completion of the metric: {0} in processor time".format(time_end - time_start)

# Second Metric
cursor = connection.cursor()
time_start = time.clock()
get_tweet_location_statics(cursor)
time_end = time.clock()
cursor.close()
print "Time taken to completion of the metric: {0} in processor time".format(time_end - time_start)

# Third Metric
cursor = connection.cursor()
time_start = time.clock()
get_average_of_tweets_per_hour(cursor)
time_end = time.clock()
cursor.close()
print "Time taken to completion of the metric: {0} in processor time".format(time_end - time_start)

# Fourth Metric
cursor = connection.cursor()
time_start = time.clock()
get_average_of_followers(cursor)
time_end = time.clock()
cursor.close()
print "Time taken to completion of the metric: {0} in processor time".format(time_end - time_start)

close_connection(connection)
