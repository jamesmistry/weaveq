# -*- coding: utf-8 -*-

from __future__ import print_function
import argparse
import json
import os
import time
import elasticsearch
from elasticsearch_dsl import Index
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl import DocType

def elastic_post_from_file(filename, hostname = "127.0.0.1", port = 9200):
    records = {}
    with open(filename, "r") as importFile:
        records = json.load(importFile)

    elastic_post(records, hostname, port)

def elastic_post(records, hostname = "127.0.0.1", port = 9200):

    connections.create_connection(hosts=["{0}:{1}".format(hostname, port)], timeout=20)

    observed_indices = {}

    successfulRecords = 0
    for record in records:
        if ("doctype" in record):
            indexName = record["doctype"]
            idx = Index(indexName)
            if (indexName not in observed_indices):
                if (idx.exists()):
                    print("Existing index deleted: {0}".format(indexName))
                    idx.delete()
                observed_indices[indexName] = None

            try:
                doc = DocType(**record)
                doc.save(index=indexName)
                successfulRecords += 1
            except elasticsearch.exceptions.RequestError as e:
                print("Request error on document {0}: {1}".format(str(record), str(e)), file=os.stderr)

    print("Loaded {0} record(s) out of {1}".format(successfulRecords, len(records)))

def main():
    parser = argparse.ArgumentParser(description='Import test data into Elasticsearch.')
    parser.add_argument('-f', '--filename', help='Filename of the JSON to import', required=True)
    parser.add_argument('-n', '--hostname', help='Elasticsearch hostname (default = 127.0.0.1)', default="127.0.0.1", required=False)
    parser.add_argument('-p', '--port', help='Elasticsearch port (default = 9200)', type=int, default=9200, required=False)
    args = parser.parse_args()

    elastic_post_from_file(args.filename, args.hostname, args.port)

if (__name__ == "__main__"):
    main()
