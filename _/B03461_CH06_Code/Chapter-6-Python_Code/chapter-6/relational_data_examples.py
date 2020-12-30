__author__ = 'bharvi'
from elasticsearch import Elasticsearch
import json

#Creating Elasticsearch Client
es = Elasticsearch('localhost:9200')

def create_index_with_nested_mapping(index_name, doc_type):
    '''
    Function to create index with nested mapping
    :param index_name: Name of the index to be created
    :param doc_type: Name of document type to be created
    '''
    doc_mapping = {
      "properties": {
        "user": {
          "type": "object",
          "properties": {
            "screen_name": {
              "type": "string"
            },
            "followers_count": {
              "type": "integer"
            },
            "created_at": {
              "type": "date"
            }
          }
        },
        "tweets": {
          "type": "nested",
          "properties": {
            "id": {
              "type": "string"
            },
            "text": {
              "type": "string"
            },
            "created_at": {
              "type": "date"
            }
          }
        }
      }
    }
    body = dict()
    mapping = dict()
    mapping[doc_type] = doc_mapping
    body['mappings'] = mapping
    es.indices.create(index=index_name, body = body)
    print 'index created successfully'

def index_nested_doc(index_name, doc_type):
    document = {
      "user": {
        "screen_name": "d_bharvi",
        "followers_count": "2000",
        "created_at": "2012-06-05"
      },
      "tweets": [
        {
          "id": "121223221",
          "text": "understanding nested relationships",
          "created_at": "2015-09-05"
        },
        {
          "id": "121223222",
          "text": "NoSQL databases are awesome",
          "created_at": "2015-06-05"
        }
      ]
    }
    es.index(index=index_name, doc_type=doc_type, body=document, id=2333)
def find_nested_docs(index_name, doc_type, nested_field):
    '''
    Function for querying nested documents
    :param index_name: Name of the index to be searched
    :param doc_type: Name of document type to be searched
    :param nested_field: path of the nested field ('tweets in our examples')
    '''
    query = {
      "query": {
        "nested": {
          "path": nested_field,
          "query": {
            "bool": {
              "must": [
                {
                  "match": {
                    "tweets.text": "NoSQL"
                  }
                },
                {
                  "term": {
                    "tweets.created_at": "2015-09-05"
                  }
                }
              ]
            }
          }
        }
      }
    }
    response = es.search(index=index_name, doc_type=doc_type, body=query)
    for hit in response['hits']['hits']:
        print json.dumps(hit.get('_source'))

def nested_aggregation(index_name, doc_type, nested_field):
    '''
    Function for performing nested aggregations
    :param index_name: Name of the index to be searched
    :param doc_type: Name of document type to be searched
    :param nested_field: path of the nested field ('tweets in our examples')
    '''
    query = {
      "aggs": {
        "NESTED_DOCS": {
          "nested": {
            "path": nested_field
          },"aggs": {
            "TWEET_TIMELINE": {
              "date_histogram": {
                "field": "tweets.created_at",
                "interval": "day"
              }
            }
          }
        }
      }
    }
    response = es.search(index=index_name, doc_type=doc_type, body=query, search_type='count')
    for bucket in response['aggregations']['NESTED_DOCS']['TWEET_TIMELINE']['buckets']:
        print bucket['key'],bucket['key_as_string'], bucket['doc_count']

def reverse_nested_aggregation(index_name, doc_type, nested_field):
    '''
    Function for performing  reverse nested aggregations
    :param index_name: Name of the index to be searched
    :param doc_type: Name of document type to be searched
    :param nested_field: path of the nested field ('tweets in our examples')
    '''
    query = {
      "aggs": {
        "NESTED_DOCS": {
          "nested": {
            "path": nested_field
          },
          "aggs": {
            "TWEET_TIMELINE": {
              "date_histogram": {
                "field": "tweets.created_at",
                "interval": "day"
              },
              "aggs": {
                "USERS": {
                  "reverse_nested": {},
                  "aggs": {
                    "UNIQUE_USERS": {
                      "cardinality": {
                        "field": "user.screen_name"
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
    response = es.search(index=index_name, doc_type=doc_type, body=query, search_type='count')
    for bucket in response['aggregations']['NESTED_DOCS']['TWEET_TIMELINE']['buckets']:
        print bucket['key'],bucket['key_as_string'], bucket['doc_count']
        print bucket['USERS']['UNIQUE_USERS']['value']
        print bucket['USERS']['doc_count']

def index_nested_doc(index_name, doc_type):
    document = {
      "user": {
        "screen_name": "d_bharvi",
        "followers_count": "2000",
        "created_at": "2012-06-05"
      },
      "tweets": [
        {
          "id": "121223221",
          "text": "understanding nested relationships",
          "created_at": "2015-09-05"
        },
        {
          "id": "121223222",
          "text": "NoSQL databases are awesome",
          "created_at": "2015-06-05"
        }
      ]
    }
    print es.index(index=index_name, doc_type=doc_type, body=document, id=2333)

def create_index_with_parent_child_mapping(index_name, parent_type, child_type):
    '''
    Function to create index with nested mapping
    :param index_name: Name of the index to be created
    :param parent_type: Name of parent type to be created
    :param child_type: Name of child_type type to be created
    '''
    child_doc_mapping = {
      "_parent": {
        "type": parent_type
      },
      "properties": {
        "text":{"type": "string"},
        "created_at":{"type": "date"}
      }
    }
    body = dict()
    mapping = dict()
    mapping[child_type] = child_doc_mapping
    body['mappings'] = mapping
    es.indices.create(index=index_name, body = body)

    parent_doc_mapping = {
      "properties": {
        "screen_name":{"type": "string"},
        "created_at":{"type": "date"}
      }
    }
    body = dict()
    mapping = dict()
    # mapping[parent_type] = parent_doc_mapping
    body['mappings'] = mapping
    es.indices.put_mapping(index=index_name, doc_type=parent_type, body = parent_doc_mapping)

    print 'index created successfully'

def index_parent_child_docs(index_name, parent_type, child_type):
    '''
    Function to index parent and child docs
    :param index_name: Name of the index
    :param parent_type: Name of parent type
    :param child_type: Name of child_type type
    '''
    parent_doc = dict()
    parent_doc['screen_name'] = 'd_bharvi'
    parent_doc['followers_count"'] = 2000
    parent_doc['create_at"'] = '2012-05-30'

    child_doc = dict()
    child_doc['text'] = 'learning parent-child concepts'
    child_doc['created_at'] = '2015-10-30'

    es.index(index=index_name, doc_type=parent_type, body=parent_doc, id='64995604')
    es.index(index=index_name, doc_type=child_type, body=child_doc, id='2333', parent= '64995604')

def find_parent_by_child(index_name, parent_type, child_type):
    '''
    Example function for has_child query to return parent documents
    :param index_name: Name of the index to be searched
    :param parent_type: Name of parent type to be returned ('users in our examples')
    :param child_type: Name of child type to be searched ('tweets in our examples')
    '''
    query = {
      "query": {
        "has_child": {
          "type": child_type,
          "query": {
            "match": {
              "text": "elasticsearch"
            }
          }
        }
      }
    }
    response = es.search(index=index_name, doc_type=parent_type, body=query)
    for hit in response['hits']['hits']:
        print json.dumps(hit.get('_source'))

def find_child_by_parent(index_name, parent_type, child_type):
    '''
    Example function for has_parent query to return child documents
    :param index_name: Name of the index to be searched
    :param parent_type: Name of parent type to be searched ('users in our examples')
    :param child_type: Name of child type to be returned ('tweets in our examples')
    '''
    query = {
      "query": {
        "has_parent": {
          "type": parent_type,
          "query": {
            "range": {
              "followers_count": {
                "gte": 200
              }
            }
          }
        }
      }
    }
    response = es.search(index=index_name, doc_type=child_type, body=query)
    for hit in response['hits']['hits']:
        print json.dumps(hit.get('_source'))

if __name__ == "__main__":
    create_index_with_nested_mapping('twitter_nested','users')
    index_nested_doc('twitter_nested','users')
    find_nested_docs('twitter_nested', 'users', 'tweets')
    nested_aggregation('twitter_nested', 'users', 'tweets')
    reverse_nested_aggregation('twitter_nested', 'users', 'tweets')
    create_index_with_parent_child_mapping('twitter_parent_child','users', 'tweets')
    index_parent_child_docs('twitter_parent_child','users', 'tweets')
    find_parent_by_child('twitter_parent_child', 'users', 'tweets')
    find_child_by_parent('twitter_parent_child', 'users', 'tweets')

