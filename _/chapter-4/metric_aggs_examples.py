__author__ = 'bharvi'

from elasticsearch import Elasticsearch
es = Elasticsearch()

def find_follower_count_stats():
    '''
    This method is an example of stats aggregation and generates stats on user.follower_count field
    :return:
    '''
    query = {
     "aggs": {
       "follower_counts_stats": {
         "stats": {
           "field": "user.followers_count"
         }
       }
     }
    }

    res = es.search(index='twitter', doc_type='tweets', body=query, search_type= 'count')
    print res['aggregations']['follower_counts_stats']['max']
    print res['aggregations']['follower_counts_stats']['avg']

def find_follower_count_extended_stats():
    '''
    This method is an example of extended_stats aggregation and generates stats on user.follower_count field
    :return:
    '''
    query = {
     "aggs": {
       "follower_counts_stats": {
         "extended_stats": {
           "field": "user.followers_count"
         }
       }
     }
    }

    res = es.search(index='twitter', doc_type='tweets', body=query, search_type= 'count')
    print res['aggregations']['follower_counts_stats']['max']
    print res['aggregations']['follower_counts_stats']['avg']

def find_min_follower_count():
    '''
    This method is an example of min aggregation and finds minimum value on user.follower_count field
    :return:
    '''
    query = {
      "aggs": {
        "min_follower_count": {
          "min": {
            "field": "user.followers_count"
          }
        }
      }
    }
    res = es.search(index='twitter', doc_type='tweets', body=query, search_type= 'count')
    print res['aggregations']['min_follower_count']['value']

def find_unique_users_count():
    '''
    This method is an example of cardinalty aggregation and generates count of unique screen names of the user
    :return:
    '''
    query = {
      "aggs": {
        "unique_users": {
          "cardinality": {
            "field": "user.screen_name"
          }
        }
      }
    }
    res = es.search(index='twitter', doc_type='tweets', body=query, search_type= 'count')
    print res['aggregations']['unique_users']['value']