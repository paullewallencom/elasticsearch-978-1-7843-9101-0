__author__ = 'bharvi'
from elasticsearch import Elasticsearch
from elasticsearch import helpers

#Create Elasticsearch Connection
es = Elasticsearch()

def get_data_from_scan_scroll(source_index, source_doc_type, destination_index, destination_doc_type, query=None):
    """
    Function for getting data from elasticsearch using scan and scroll
    :param source_index: Index from which data needs to be fetched from
    :param source_doc_type: Doc Type which needs to be fetched
    :param destination_index: Index in which data needs to be indexed
    :param destination_doc_type: Doc type in which data needs to be indexed
    :param query: Query for fetching data, defaults to match all
    :return:
    """
    if query is None:
        query = {"query":{"match_all":{}}}
    documents = []

    try:
        print 'getting scroll id'
        resp = es.search(index=source_index, doc_type=source_doc_type, body=query, search_type="scan",
                         scroll='100s', size=100)
        scroll_count = 0
        while True:
            print 'scrolling for ',str(scroll_count)+' time'
            resp = es.scroll(resp['_scroll_id'], scroll='100s')
            if len(resp['hits']['hits']) == 0:
                print 'data re-indxing completed..!!'
                break
            else:
                documents.extend(resp['hits']['hits'])
                #Send the documents for reindexing and start another loop for fetching remaining docs
                perform_bulk_index(destination_index, destination_doc_type, documents)
                documents = []

    except Exception as e:
            print 'got an exception', str(e)

def perform_bulk_index(destination_index, destination_doc_type, documents):
    """
    Function for indexing documents in the bulk
    :param destination_index: Index in which data needs to be indexed
    :param destination_doc_type: Doc type in which data needs to be indexed
    :param documents:
    :return:
    """
    actions = []
    for document in documents:
        actions.append({
               '_op_type': 'index',
               '_index': destination_index,
               '_type': destination_doc_type,
               '_id': document['_id'],
               '_source': document['_source']
                })
    try:
        helpers.bulk(es, actions, request_timeout=100)
    except Exception as e:
        print "bulk index raised exception", str(e)
    print 'done'
