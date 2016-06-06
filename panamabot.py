import sqlite3
import tweepy
import os

conn = sqlite3.connect('./panama.db')
cur = conn.cursor()

def rand_get():
    cur.execute('select * from all_edges order by random() limit 1;')
    return cur.fetchall()[0]

def search_id(id):
    tables = [
        'Addresses',
        'Entities',
        'Intermediaries',
        'Officers'
    ]
    for table in tables:
        cur.execute('select * from %s where id=%d;' % (table,id))
        result = cur.fetchall()
        if len(result) != 0 :
            return result[0][1]
    return ''
def translate(rel_type):
    cur.execute('select * from translation where rel_type="%s"'%rel_type)
    return cur.fetchall()[0][1]
    
def tweet_gen():
    while True:
        item = rand_get()
        node1 = search_id(item[1])
        type = translate(item[2])
        node2 = search_id(item[3])
        result = type.format(node1,node2)
        if type != '' and len(result) <= 140:
            return result

def tweet(api,txt):
    api.update_status(txt)

def get_friends(api):
    result = set()
    for friend in tweepy.Cursor(api.friends).items():
        result.add(friend.id)
    return  result

def get_followers(api):
    result = set()
    for follower in tweepy.Cursor(api.followers).items():
        result.add(follower.id)
    return  result

def followback(api):
    friends = get_friends(api)
    followers = get_followers(api)
    task = followers.difference(friends)
    for id in task:
        api.get_user(id).follow()

consumer_key = os.environ.get('TWITTER_CONSUMER_KEY')
consumer_secret = os.environ.get('TWITTER_CONSUMER_SECRET')
access_token = os.environ.get('TWITTER_ACCESS_TOKEN')
access_token_secret = os.environ.get('TWITTER_ACCESS_TOKEN_SECRET')

auth = tweepy.OAuthHandler(consumer_key,consumer_secret)
auth.set_access_token(access_token,access_token_secret)
api = tweepy.API(auth,wait_on_rate_limit=True)
tweet(api,tweet_gen())
followback(api)