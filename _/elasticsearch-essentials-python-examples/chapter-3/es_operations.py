__author__ = 'bharvi'
from elasticsearch import Elasticsearch
from time import time
from config import index_settings, doc_mapping
import json

#Creating Elasticsearch Client
es = Elasticsearch('localhost:9200')

def check_index_existence(index_name):
    '''
    Function for checking the existence of an index
    :param index_name: Name of the index to be created
    :param doc_type: Name of document type to be created
    :return: False if index does not exist else True
    '''
    if not es.indices.exists(index=index_name):
        return False
    else:
        return True

def create_index(index_name, doc_type):
    '''
    Function for creating an index with settings and mappings
    :param index_name: Name of the index to be created
    :return:
    '''
    body = {}
    mapping = {}
    mapping[doc_type] = doc_mapping
    body['settings'] = index_settings
    body['mappings'] = mapping
    if not check_index_existence(index_name):
        es.indices.create(index=index_name, body = body)
        time.sleep(2)
        print 'index created successfully'

def index_document(index_name, doc_type, doc_id):
    '''
    Function for index a document
    :param index_name: Name of the index
    :param doc_type: Name of document type
    :param doc_id: Unique id to be set for the document
    :return:
    '''
    doc1 = {
            'name': 'Elasticsearch Essentials',
            'category': ['Big Data', 'search engines', 'Analytics'],
            'Publication': 'Packt-Pub',
            'Publishing Date': '2015-31-12'
            }
    if doc_id is not None:
        doc_id = doc_id

    es.index(index=index_name, doc_type=doc_type, body=doc1, id=doc_id)

def get_document(index_name, doc_type, doc_id):
    '''
    Function for fetching a single document
    :param index_name: Name of the index
    :param doc_type: Name of document type
    :param doc_id: Document id to be retrieved
    :return:
    '''
    response = es.get(index=index_name, doc_type=doc_type, id=doc_id, ignore=404)
    print response
    #Access fields from the response
    print response.get('_source').get('category')

def update_document_append_mode(index_name, doc_type, field_name, append_value, doc_id):
    '''
    function for partially updating  document in append mode
    :param index_name: Name of the index
    :param doc_type: Name of document type
    :param field_name: Name of the array field to be updated
    :param append_value: Value which need to be appended in the array
    :param doc_id: Document id to be updated
    :return:
    '''
    script = {"script" : "ctx._source."+field_name+" +="+ "parameter",
                "params" : {
                    "parameter" : append_value
                }
            }
    es.update(index=index_name, doc_type=doc_type, body=script, id=doc_id)

def update_document_replace_mode(index_name, doc_type, field_name, new_value, doc_id):
    '''
    function for partially updating  document in append mode
    :param index_name: Name of the index
    :param doc_type: Name of document type
    :param field_name: Name of the array field to be updated
    :param append_value: Value which need to be appended in the array
    :param doc_id: Document id to be updated
    :return:
    '''
    script ={"script" : "ctx._source."+field_name+" = \"" + new_value+"\""}
    print script
    es.update(index=index_name, doc_type=doc_type, body=script, id=doc_id, ignore=404)


def check_document_existence(index_name, doc_type, doc_id):
    '''
    Function for checking if a document exists ot not in the given index
    :param index_name: Name of the index
    :param doc_type: Name of document type
    :param doc_id: Document id whose existence need to be checked
    :return:
    '''
    if not es.exists(index=index_name, doc_type=doc_type, id=doc_id):
        return False
    else:
        return True

def delete_document(index_name, doc_type, doc_id):
    '''
    Function for deleting a document
    :param index_name: Name of the index
    :param doc_type: Name of document type
    :param doc_id: Document id which is to be deleted
    :return:
    '''
    es.delete(index=index_name, doc_type=doc_type, id=doc_id, ignore=404)


def search_documents(index_name, doc_type, query=None):
    '''
    Function for performing search requests using Query-DSL queries
    :param index_name: Name of the index
    :param doc_type: Name of document type
    :param query: Query in json format
    :return:
    '''
    if query is None:
        query = {
         "query":{
           "match":{"text":"crime"}
         }
        }

    response = es.search(index=index_name, doc_type=doc_type, body=query, size=2, request_timeout=20)
    for hit in response['hits']['hits']:
        print json.dumps(hit.get('_source'))

if __name__ == '__main__':
    index_name = 'test'
    doc_type = 'test'
    create_index(index_name,doc_type)
    index_document(index_name, doc_type, '1')
    get_document(index_name, doc_type, '1')
    update_document_replace_mode(index_name, doc_type, 'Publishing_Date', '2016-20-01', '1')
    update_document_append_mode(index_name, doc_type, 'category', 'search', '1')
    search_documents(index_name, doc_type)