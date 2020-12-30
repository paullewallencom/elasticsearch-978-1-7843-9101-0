__author__ = 'bharvi'

from elasticsearch import Elasticsearch
es = Elasticsearch()

def min_aggregation():
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

def max_aggregation():
    '''
    This method is an example of max aggregation and finds maximum value on user.follower_count field
    :return:
    '''
    query = {
      "aggs": {
        "min_follower_count": {
          "max": {
            "field": "user.followers_count"
          }
        }
      }
    }
    res = es.search(index='twitter', doc_type='tweets', body=query, search_type= 'count')
    print res['aggregations']['min_follower_count']['value']
def avg_aggregation():
    '''
    This method is an example of avg aggregation and finds average value on user.follower_count field
    :return:
    '''
    query = {
      "aggs": {
        "min_follower_count": {
          "avg": {
            "field": "user.followers_count"
          }
        }
      }
    }
    res = es.search(index='twitter', doc_type='tweets', body=query, search_type= 'count')
    print res['aggregations']['min_follower_count']['value']
def sum_aggregation():
    '''
    This method is an example of sum aggregation and finds sum value on user.follower_count field
    :return:
    '''
    query = {
      "aggs": {
        "min_follower_count": {
          "sum": {
            "field": "user.followers_count"
          }
        }
      }
    }
    res = es.search(index='twitter', doc_type='tweets', body=query, search_type= 'count')
    print res['aggregations']['min_follower_count']['value']

def stats_aggregation():
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

def extended_stats_aggregation():
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

def cardinality_aggregation():
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