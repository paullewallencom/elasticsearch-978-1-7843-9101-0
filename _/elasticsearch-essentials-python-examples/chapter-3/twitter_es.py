__author__ = 'bharvi'
"""This code uses the Twitter Streaming API, via the tweepy library,
to pull in tweets and pushes to elasticsearch.
"""
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from elasticsearch import Elasticsearch
import config
import json
from dateutil.parser import parse

es = Elasticsearch('localhost:9200')

class StdOutListener(StreamListener):
    """A listener handles tweets that are received from the stream.
    This listener dumps the tweets into elasticsearch
    """
    counter = 0
    total_docs_to_be_indexed = 1000

    def on_data(self, data):
        '''
        Actions to do after documents are fetched from twitter
        :param data:
        :return:
        '''
        print data
        while self.total_docs_to_be_indexed > self.counter:
            tweet = json.loads(data)
            tweet['created_at'] = parse(tweet['created_at'])
            tweet['user']['created_at'] = parse(tweet['user']['created_at'])
            self.index_tweet(tweet)
            self.counter += 1
            return True

    def index_tweet(self, tweet):
        '''
        index tweets in elasticsearch by setting tweet id as unique id for documents
        :param tweet: A single tweet document
        :return:
        '''
        es.index(index='twitter', doc_type='tweets', id=tweet['id_str'], body=tweet)

    def on_error(self, status):
        print status


if __name__ == '__main__':

    listener = StdOutListener()
    auth = OAuthHandler(config.consumer_key, config.consumer_secret)
    auth.set_access_token(config.access_token, config.access_token_secret)
    stream = Stream(auth, listener)
    stream.filter(track=['crime', 'blast', 'earthquake', 'riot', 'politics'])