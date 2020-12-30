'''
This file contains examples of index settings and mappings which will used in the es_operations.py for creating index
'''

#Index Settings
index_settings = {
    "number_of_shards": 1,
    "number_of_replicas": 1,
    "index": {
        "analysis": {
            "analyzer": {
                "keyword_analyzed": {
                    "type": "custom",
                    "filter": [
                      "lowercase",
                      "asciifolding"
                    ],
                    "tokenizer": "keyword"
                    }
                }
            }
        }
    }

# Document Mapping
doc_mapping = {
    "_all": {
        "enabled": False
    },
    "properties": {
        "skills": {
            "type": "string",
            "index": "analyzed",
            "analyzer": "keyword_analyzed",
        }
    }
}

#Put tokens and keys of twitter account here
consumer_key = 'xxxxxxxxxxxxxxxxxxxxxxx'
consumer_secret = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
access_token = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
access_token_secret = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
