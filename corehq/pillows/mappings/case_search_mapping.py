from pillowtop.es_utils import (
    CASE_SEARCH_HQ_INDEX_NAME,
    ElasticsearchIndexInfo,
)

from corehq.apps.es.case_search import case_search_adapter
from corehq.apps.es.client import Tombstone
from corehq.pillows.core import DATE_FORMATS_ARR, DATE_FORMATS_STRING
from corehq.util.elastic import prefix_for_tests

CASE_SEARCH_INDEX = case_search_adapter.index_name
CASE_SEARCH_ALIAS = prefix_for_tests('case_search')

CASE_SEARCH_MAPPING = {
    "_all": {
        "enabled": False
    },
    "_meta": {
        "comment": "",
        "created": "2016-03-29 @frener"
    },
    "date_detection": False,
    "date_formats": DATE_FORMATS_ARR,
    "dynamic": False,
    "properties": {
        "@indexed_on": {
            "format": DATE_FORMATS_STRING,
            "type": "date"
        },
        "case_properties": {
            "dynamic": False,
            "type": "nested",
            "properties": {
                "key": {
                    "fields": {
                        "exact": {
                            "type": "keyword"
                        }
                    },
                    "type": "text"
                },
                "value": {
                    "fields": {
                        "date": {
                            "format": DATE_FORMATS_STRING,
                            "ignore_malformed": True,
                            "type": "date"
                        },
                        "exact": {
                            "ignore_above": 8191,
                            "null_value": "",
                            "type": "keyword"
                        },
                        "numeric": {
                            "ignore_malformed": True,
                            "type": "double"
                        },
                        "phonetic": {
                            "analyzer": "phonetic",
                            "type": "text"
                        }
                    },
                    "type": "text"
                },
                "geopoint_value": {
                    "type": "geo_point"
                }
            }
        },
        "closed": {
            "type": "boolean"
        },
        "closed_by": {
            "type": "keyword"
        },
        "closed_on": {
            "format": DATE_FORMATS_STRING,
            "type": "date"
        },
        "doc_type": {
            "type": "keyword"
        },
        "domain": {
            "fields": {
                "exact": {
                    "type": "keyword"
                }
            },
            "type": "text"
        },
        "external_id": {
            "type": "keyword"
        },
        "indices": {
            "dynamic": False,
            "type": "nested",
            "properties": {
                "doc_type": {
                    "type": "keyword"
                },
                "identifier": {
                    "type": "keyword"
                },
                "referenced_id": {
                    "type": "keyword"
                },
                "referenced_type": {
                    "type": "keyword"
                },
                "relationship": {
                    "type": "keyword"
                }
            }
        },
        "location_id": {
            "type": "keyword"
        },
        "modified_on": {
            "format": DATE_FORMATS_STRING,
            "type": "date"
        },
        "name": {
            "fields": {
                "exact": {
                    "type": "keyword"
                }
            },
            "type": "text"
        },
        "opened_by": {
            "type": "keyword"
        },
        "opened_on": {
            "format": DATE_FORMATS_STRING,
            "type": "date"
        },
        "owner_id": {
            "type": "keyword"
        },
        "server_modified_on": {
            "format": DATE_FORMATS_STRING,
            "type": "date"
        },
        "type": {
            "fields": {
                "exact": {
                    "type": "keyword"
                },
            },
            "type": "text"
        },
        "user_id": {
            "type": "keyword"
        },
        Tombstone.PROPERTY_NAME: {
            "type": "boolean"
        },
    }
}


CASE_SEARCH_INDEX_INFO = ElasticsearchIndexInfo(
    index=CASE_SEARCH_INDEX,
    alias=CASE_SEARCH_ALIAS,
    type=case_search_adapter.type,
    mapping=CASE_SEARCH_MAPPING,
    hq_index_name=CASE_SEARCH_HQ_INDEX_NAME,
)
