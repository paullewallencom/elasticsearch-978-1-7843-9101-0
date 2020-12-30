__author__ = 'bharvi'
from elasticsearch import Elasticsearch
import json
es = Elasticsearch()

def create_index_with_geoshape_mapping(index_name, doc_type):
    """
    Function to create index with geo_shape
    :param index_name: Name of the index to be created
    :param doc_type: Name of document type to be created
    """
    doc_mapping = {
      "properties": {
       "location": {
            "type": "geo_shape"
            }
        }
    }
    body = {}
    mapping = {}
    mapping[doc_type] = doc_mapping
    body['mappings'] = mapping
    es.indices.create(index=index_name, body = body)
    print 'index created successfully'

def index_geoshape_data(index_name, doc_type):
    """
    Function for providing examples of indexing geo_point data in string, object and array format
    :param index_name:Name of the index
    :param doc_type:name of the document type
    :return:
    """
    doc = dict()
    location = dict()
    location['coordinates'] = [13.400544, 52.530286]
    doc['location'] = location
    doc['location']['type'] = 'Point'
    es.index(index=index_name, doc_type=doc_type, body=doc)

def find_on_linestring(index_name, doc_type):
    """
    Linestring geoshape search example
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
            "geo_shape": {
              "location": {
                "shape": {
                  "type": "linestring",
                  "coordinates": [[ 13.400544,52.530286],[13.4006,52.5303]]
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
        print hit.get('_source')

def find_on_envelope(index_name, doc_type):
    """
    Envelope search example, uses top left and bottom right coordinates for searching
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
            "geo_shape": {
              "location": {
                "shape": {
                  "type": "envelope",
                  "coordinates": [[13,53],[14,52]]
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
        print hit.get('_source')

if __name__ == "__main__":
    #test functions
    geo_shape_index_name = 'geo_point_test'
    geo_shape_doc_type = 'geo_points'
    create_index_with_geoshape_mapping(geo_shape_index_name,geo_shape_doc_type)
    index_geoshape_data(geo_shape_index_name, geo_shape_doc_type)
    find_on_linestring(geo_shape_index_name, geo_shape_doc_type)
    find_on_envelope(geo_shape_index_name, geo_shape_doc_type)