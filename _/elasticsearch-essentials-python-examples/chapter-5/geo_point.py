__author__ = 'bharvi'
from elasticsearch import Elasticsearch
import json
es = Elasticsearch()

def create_index_with_geopoint_mapping(index_name, doc_type):
    """
    Function to create index with nested mapping
    :param index_name: Name of the index to be created
    :param doc_type: Name of document type to be created
    """
    doc_mapping = {
      "properties": {
       "location": {
            "type": "geo_point"
            }
        }
    }
    body = dict()
    mapping = dict()
    mapping[doc_type] = doc_mapping
    body['mappings'] = mapping
    es.indices.create(index=index_name, body = body)
    print 'index created successfully'

def index_geopoint_data(index_name, doc_type):
    '''
    Function for providing examples of indexing geo_point data in string, object and array format
    :param index_name:Name of the index
    :param doc_type:name of the document type
    :return:
    '''
    #index lat_lon in string form
    doc = dict()
    location = "29.9560, 78.1700"
    doc['location'] = location
    doc['name'] = 'delhi'
    es.index(index=index_name, doc_type=doc_type, body=doc)

    #index lat_lon in object form
    location = dict()
    location['lat'] = 28.67
    location['lon'] = 77.42
    doc['location'] = location
    es.index(index=index_name, doc_type=doc_type, body=doc)

    #index lat_lon in array form
    location = list()
    location.append(77.42)
    location.append(28.67)
    doc['location'] = location
    es.index(index=index_name, doc_type=doc_type, body=doc)

def find_by_distance(index_name, doc_type):
    """
    Example of geo_distance_ query.
    Finds all the documents with a combination of a given query and within a given distance from a location's geo_point
    :param index_name:
    :param doc_type:
    :return:
    """
    query = {
      "query": {
        "bool": {
          "must": {
            "match_all": {}
          },
          "filter": {
            "geo_distance": {
              "distance": "12km",
              "location": {
                "lat": 28.67,
                "lon": 77.42
              }
            }
          }
        }
      }
    }
    response = es.search(index=index_name, doc_type=doc_type, body=query)
    print 'total documents found', response['hits']['total']
    for hit in response['hits']['hits']:
        print json.dumps(hit.get('_source'))

def find_by_range(index_name, doc_type):
    """
    Example of geo_distance_range query.
    Finds all the documents with a combination of a given query and a given range from a location's geo_point
    :param index_name:
    :param doc_type:
    :return:
    """
    query = {
      "query": {
        "bool": {
          "must": {
            "match_all": {}
          },
          "filter": {
            "geo_distance_range": {
              "from": "100km",
              "to": "4000km",
              "location": [77.42,28.67]
            }
          }
        }
      }
    }
    response = es.search(index=index_name, doc_type=doc_type, body=query)
    print 'total documents found', response['hits']['total']
    for hit in response['hits']['hits']:
        print json.dumps(hit.get('_source'))

def find_by_bounding_box(index_name, doc_type):
    """
    Example of geo_bounding_box query.
    Finds all the documents with a combination of a given query and a given bounding box,
    where a bounding box is defined using top_left and bottom_right points
    :param index_name:
    :param doc_type:
    :return:
    """
    query= {
      "query": {
        "bool": {
          "must": {
            "match_all": {}
          },
          "filter": {
            "geo_bounding_box": {
              "location": {
                "top_left": {
                  "lat": 68.91,
                  "lon": 35.60
                },
                "bottom_right": {
                  "lat": 7.80,
                  "lon": 97.29
                }
              }
            }
          }
        }
      }
    }
    response = es.search(index=index_name, doc_type=doc_type, body=query)
    print 'total documents found', response['hits']['total']
    for hit in response['hits']['hits']:
        print json.dumps(hit.get('_source'))

def sort_by_distance(index_name, doc_type):
    """
    find all the restaurants from the list of restaurants available in your index in sorted order with respect to your
    current location lat-lon point (28.67 and 77 in our example) & which sells chinese cuisine.
    :param index_name:
    :param doc_type:
    :return:
    """
    query = {
          "query": {
            "term": {
              "dish_name": {
                "value": "chinese"
              }
            }
          },
          "sort": [
            {
              "_geo_distance": {
                "location": [
                  28.67,
                  77
                ],
                "order": "asc",
                "unit": "km"
              }
            }
          ]
        }
    response = es.search(index=index_name, doc_type=doc_type, body=query)
    for hit in response['hits']['hits']:
        print json.dumps(hit.get('_source'))

def geo_aggregation(index_name, doc_type, origin):
    """
    Function for performing  geo distance aggregations to create buckets of documents with
    ranges from 0-50 km, 50-200 km and above 200 km from a specified origin point
    :param index_name: Name of the index to be searched
    :param doc_type: Name of document type to be searched
    """
    query = {
      "aggs": {
        "news_hotspots": {
          "geo_distance": {
            "field": "location",
            "origin": origin,
             "unit": "km",
             "distance_type": "plane",
            "ranges": [
              {
                "to": 50
              },
              {
                "from": 50, "to": 200
              },
              {
                "from": 200
              }
            ]
          }
        }
      },"size": 0
    }
    response = es.search(index=index_name, doc_type=doc_type, body=query, search_type='count')
    for bucket in response['aggregations']['news_hotspots']['buckets']:
        print bucket['key'], bucket['doc_count']

if __name__ == "__main__":
    #test functions
    geo_point_index_name = 'geo_point'
    geo_point_doc_type = 'geo_points'
    location_origin_points = "28.61, 77.23"

    # create_index_with_geopoint_mapping(geo_point_index_name,geo_point_doc_type)
    index_geopoint_data(geo_point_index_name,geo_point_doc_type)
    find_by_distance(geo_point_index_name,geo_point_doc_type)
    find_by_range(geo_point_index_name,geo_point_doc_type)
    find_by_bounding_box(geo_point_index_name,geo_point_doc_type)
    sort_by_distance(geo_point_index_name,geo_point_doc_type)
    geo_aggregation(geo_point_index_name,geo_point_doc_type,location_origin_points)