__author__ = 'bharvi'

from elasticsearch import Elasticsearch
es = Elasticsearch('localhost:9200')

def score_by_weight(index_name, doc_type):
    '''
    Example function for scoring document using weight function
    :param index_name: Name of the index to be searched
    :param doc_type: Name of document type to be searched
    '''
    query = {
     "query": {
       "function_score": {
         "query": {
           "term": {
             "skills": {
               "value": "java"
             }
           }
         },
         "functions": [
           {
             "filter": {
               "term": {
                 "skills": "python"
               }
             },
             "weight": 2
           }
         ],
         "boost_mode": "replace"
       }
     }
    }
    response = es.search(index=index_name, doc_type=doc_type, body=query)
    for hit in response['hits']['hits']:
        print hit.get('_source')

def score_by_field_value_factor(index_name, doc_type):
    '''
    Example function for scoring document using field value factor
    :param index_name: Name of the index to be searched
    :param doc_type: Name of document type to be searched
    '''
    query = {
     "query": {
       "function_score": {
         "query": {
           "term": {
             "skills": {
               "value": "java"
             }
           }
         },
         "functions": [
           {
             "field_value_factor": {
               "field": "total_experience"
             }
           }
         ],
         "boost_mode": "multiply"
       }
     }
    }
    response = es.search(index=index_name, doc_type=doc_type, body=query)
    for hit in response['hits']['hits']:
        print hit.get('_source')

def score_by_script(index_name, doc_type):
    '''
    Example function for scoring document using groovy scripting
    :param index_name: Name of the index to be searched
    :param doc_type: Name of document type to be searched
    '''
    script = "final_score=0; " \
             "skill_array = doc['skills'].toArray(); " \
             "counter=0; " \
             "while(counter<skill_array.size()){" \
             "for(skill in skill_array_provided){" \
             "if(skill_array[counter]==skill){" \
             "final_score = final_score+doc['total_experience'].value" \
             "};" \
             "};" \
             "counter=counter+1;" \
             "};return final_score"
    params = []
    params.append('java')
    params.append('python')
    script_params = {}
    script_params['skill_array_provided'] = params

    query = {
     "query": {
       "function_score": {
         "query": {
           "term": {
             "skills": {
               "value": "java"
             }
           }
         },
         "functions": [
           {
             "script_score": {
               "params": script_params,
               "script": script
             }
           }
         ],
         "boost_mode": "replace"
       }
     }
    }
    response = es.search(index=index_name, doc_type=doc_type, body=query)
    for hit in response['hits']['hits']:
        print hit.get('_source')

def score_by_decay_functions(index_name, doc_type):
    '''
    Example function for scoring document using decay functions
    :param index_name: Name of the index to be searched
    :param doc_type: Name of document type to be searched
    '''
    query = {
     "query": {
       "function_score": {
         "query": {
           "match_all": {}
         },
         "functions": [
           {
             "exp": {
               "geo_code": {
                 "origin": {
                   "lat": 28.66,
                   "lon": 77.22
                 },
                 "scale": "100km"
               }
             }
           }
         ],"boost_mode": "multiply"
       }
     }
    }
    response = es.search(index=index_name, doc_type=doc_type, body=query)
    for hit in response['hits']['hits']:
        print hit.get('_source')

if __name__ == '__main__':

    index_name = 'profiles'
    doc_type = 'candidate'
    score_by_weight(index_name, doc_type)
    score_by_field_value_factor(index_name, doc_type)
    score_by_script(index_name, doc_type)
    score_by_decay_functions(index_name, doc_type)