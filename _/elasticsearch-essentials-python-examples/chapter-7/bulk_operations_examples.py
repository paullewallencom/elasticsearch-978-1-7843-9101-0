__author__ = 'bharvi'
import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers

#Create Elasticsearch Connection
es = Elasticsearch()


def bulk_create_example(index_name, doc_type):
    """
    Function for bulk creation.
    :param index_name: Name of the index in which documents needs to be indexed.
    :param doc_type: Name of the doc_type to be set for the documents
    :return:
    """
    #create document 1
    docs = []
    doc1 = dict()
    doc1['text'] = 'checking out search types in elasticsearch'
    doc1['created_at'] = datetime.datetime.utcnow()

    #create document 2
    doc2 = dict()
    doc2['text'] = 'bulk API is awesome'
    doc2['created_at'] = datetime.datetime.utcnow()

    docs.append(doc1)
    docs.append(doc2)
    print docs
    actions = list()
    id_generator = 0
    for doc in docs:
        """set _op_type paramter to 'index' for bulk indexing instead of bulk creation
        index names and doc type can be different inside a single bulk request"""
        id_generator += 1
        action = {
            '_index': index_name,
            '_type': doc_type,
            'id': str(id_generator),
            '_op_type': 'create',
            '_source': doc
        }
        actions.append(action)
    # Execute the es bulk api
    try:
        bulk_response = helpers.bulk(es, actions,request_timeout=100)
        print "bulk response:",bulk_response
    except Exception as e:
        print str(e)

def bulk_update_example(index_name, doc_type):
    """
    Function for bulk updates example
    :param index_name: Name of the index whose documents needs to be updated
    :param doc_type: Document type whose documents needs to be updated
    :return:
    """
    #create document 1
    docs = []
    doc1 = dict()
    doc1['text'] = 'checking out search types in elasticsearch'
    doc1['created_at'] = datetime.datetime.utcnow()

    #create document 1
    doc2 = dict()
    doc2['text'] = 'bulk API is awesome'
    doc2['created_at'] = datetime.datetime.utcnow()

    docs.append(doc1)
    docs.append(doc2)
    print docs
    actions = list()
    id_generator = 0
    for doc in docs:
        #index names and doc type can be different inside a single bulk request
        id_generator += 1
        action = {
            '_index': index_name,
            '_type': doc_type,
            '_id': str(id_generator),
            '_op_type': 'update',
            'doc': {'new_field': 'doing partial update with a new field'}
        }
        actions.append(action)
    # Execute the es bulk api
    print "going to execute bulk for created documents ",len(actions)
    try:
        bulk_indexed = helpers.bulk(es, actions,request_timeout=100)
        print "bulk response:",bulk_indexed
    except Exception as e:
        print str(e)

def bulk_delete_example(index_name, doc_type, ids_to_delete):
    """
    Function for document deletion in bulk
    :param index_name: name of the index whose documents needs to be deleted
    :param doc_type: name of the doc type whose documents needs to be deleted
    :param ids_to_delete: _id of tje documents to be deleted
    :return:
    """
    del_complete_batch = []
    for id in ids_to_delete:
        #index names and doc type can be different inside a single bulk request
        del_complete_batch.append({
            '_op_type': 'delete',
            '_index': index_name,
            '_type': doc_type,
            '_id': id,
        })
    try:
        helpers.bulk(es, del_complete_batch, request_timeout=100)
    except Exception as e:
        print str(e)

def multi_get_example(index_name, doc_type, document_ids_to_get):
    """
    Function for document deletion in bulk
    :param index_name: name of the index whose documents needs to be deleted
    :param doc_type: name of the doc type whose documents needs to be deleted
    :param document_ids_to_get: array of _id of the documents to be fetched
    :return:
    """
    query = {"ids": document_ids_to_get}
    """_source is set to be false to only return document ids so that their existence can be checked
    set it to true if you want the complete document fields to be returned"""
    exists_resp = es.mget(index=index_name,doc_type=doc_type, body=query, _source=False, request_timeout=100)
    for doc in exists_resp['docs']:
        if not doc['found']:
            print "the document", "for ",doc['_id'], "not found"
        else:
            print 'found', doc['_id']


def multi_search_example(index_name1, doc_type1, index_name2, doc_typ2):
    """
    Example dunction for sending more thanone queries to Elasticsearch in a single request using msearch endpoint
    The request can be either executed on a single index or multiple index in a single request
    :param index_name1: Name of the first index to be queried
    :param doc_type1: Name of the first doc type to be queried
    :param index_name2: Name of the second index to be queried
    :param doc_typ2: Name of the second doc type to be queried
    :return:
    """
    #Create individual request head with index name and doc type in following way
    req_head1 = {'index': index_name1, 'type': doc_type1}
    #query_request_array contains the actual queries and the head of part of that query
    query_request_array = []

    query_1 = {"query" : {"match_all" : {}}}
    query_request_array.append(req_head1)
    query_request_array.append(query_1)

    #create another request with head and body
    req_head2 = {'index': index_name2, 'type': doc_typ2}
    query_2 = {"query" : {"match_all" : {}}}
    query_request_array.append(req_head2)
    query_request_array.append(query_2)

    """Execute the request using msearch endpoint by passing the query_request_array into the body,
    you can optionally set the request_timeout too."""
    response = es.msearch(body=query_request_array)

    #The response of msearch can be parsed in following way
    for resp in response["responses"]:
        if resp.get("hits"):
            for hit in resp.get("hits").get('hits'):
                print hit["_source"]

