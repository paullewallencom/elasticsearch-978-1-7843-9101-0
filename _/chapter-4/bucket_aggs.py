__author__ = 'bharvi'
from elasticsearch import Elasticsearch
es = Elasticsearch()

def computing_nested_aggregation():
    '''
    This method is an exmaple of computing nested aggregation in combiation of query, bucket aggregation and metric.
    It gives following response:
    A bucket of hourly count of tweets with matches crime in th text field
    and based on that bucket it find the top hashtags used.
    Each hashtag bucket further calcluates top screen names who are using that hashtag
    Finally it calcluates an average tweets of the users done in their lifetime who have used this particular hashtag.
    :return:
    '''
    query = {
    "query": {
      "match": {
        "text": "crime"
      }
    },
    "aggs": {
      "hourly_timeline": {
        "date_histogram": {
          "field": "created_at",
          "interval": "hour"
        },
        "aggs": {
          "top_hashtags": {
            "terms": {
              "field": "entities.hashtags.text",
              "size": 1
            },
            "aggs": {
              "top_users": {
                "terms": {
                  "field": "user.screen_name",
                  "size": 1
                },
                "aggs": {
                  "average_tweets": {
                    "sum": {
                      "field": "user.statuses_count"
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    }

    res = es.search(index='twitter', doc_type='tweets', body=query, search_type= 'count')

    for timeline_bucket in res['aggregations']['hourly_timeline']['buckets']:
      print 'time_range', timeline_bucket['key_as_string']
      print 'tweet_count ',timeline_bucket['doc_count']
      for hashtag_bucket in timeline_bucket['top_hashtags']['buckets']:
          print '\thashtag_key ', hashtag_bucket['key']
          print '\thashtag_count ', hashtag_bucket['doc_count']
          for user_bucket in hashtag_bucket['top_users']['buckets']:
              print '\t\tscreen_name ', user_bucket['key']
              print '\t\tcount', user_bucket['doc_count']
              print '\t\taverage_tweets', user_bucket['average_tweets']['value'], '\n'

def find_status_count_ranges():
    '''
    This method calculates the range of tweet counts done by users on twitter
    :return:
    '''
    query =  {
        "aggs": {
        "status_count_ranges": {
          "range": {
            "field": "user.statuses_count",
            "ranges": [
              {
                "to": 50
              },
              {
                "from": 50,
                "to": 100
              }
            ]
          }
        }
      }
    }

    res = es.search(index='twitter', doc_type='tweets', body=query, search_type= 'count')
    for bucket in res['aggregations']['status_count_ranges']['buckets']:
        print bucket['key'], bucket['doc_count']

def generate_tweet_timeline():
    '''
    This method calculates hourly based tweet count
    :return:
    '''
    query = {
      "aggs": {
        "tweet_histogram": {
          "date_histogram": {
            "field": "created_at",
            "interval": "hour"
          }
        }
      }
    }
    res = es.search(index='twitter', doc_type='tweets', body=query, search_type= 'count')
    for bucket in res['aggregations']['tweet_histogram']['buckets']:
        print bucket['key'], bucket['key_as_string'], bucket['doc_count']



