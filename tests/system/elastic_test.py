# -*- coding: utf-8 -*-

import json
import os
import time
import logging
import unittest
import sys

from elasticsearch import Elasticsearch
from elasticsearch import ElasticsearchException
from elasticsearch_dsl import Search
import generate_data
from jiggleq.query import JiggleQ
from jiggleq.relations import F
from jiggleq.datasources import ElasticsearchDataSource
import data.load_data

tracer = logging.getLogger("jiggleq")
tracer.setLevel(logging.INFO)
tracer.addHandler(sys.stdout)

class TestResultHandler(object):
    def __init__(self):
        self.results = []
        self.invalid_record_count = 0

    def __call__(self, result, handler_output):
        self.results.append(result)

        if ("joined_rec" not in result):
            self.invalid_record_count += 1
            tracer.error("Invalid record, no joined_rec field")
            return

        if (len(result["joined_rec"]) != 333):
            self.invalid_record_count += 1
            tracer.error("Invalid record, expected join count: 333; actual join count: {0}".format(len(result["joined_rec"])))
            return

        if (result["doctype"] != "jiggleqsystest_set2"):
            self.invalid_record_count += 1
            tracer.error("Invalid record, expected doctype: jiggleqsystest_set2; actual doctype: {0}".format(result["doctype"]))
            return

        if (result["joined_rec"][0]["doctype"] != "jiggleqsystest_set1"):
            self.invalid_record_count += 1
            tracer.error("Invalid joined record, expected doctype: jiggleqsystest_set1; actual doctype: {0}".format(result["doctype"]))
            return

        if ("key_2_2" not in result):
            self.invalid_record_count += 1
            tracer.error("Invalid record, key key_2_2 doesn't exist")
            return

        if ("key_1_2" not in result["joined_rec"][0]):
            self.invalid_record_count += 1
            tracer.error("Invalid joined record, key key_1_2 doesn't exist")
            return

        if (u"ψυχοφθόρα_field_2" not in result):
            self.invalid_record_count += 1
            tracer.error(u"UTF-8 data field ψυχοφθόρα_field_2 doesn't exist")
            return

        if (result[u"ψυχοφθόρα_field_2"] != u"ψυχοφθόρα"):
            self.invalid_record_count += 1
            tracer.error(u"UTF-8 data field doesn't contain expected UTF-8 data ψυχοφθόρα")
            return

    def success(self):
        return True

    def validate(self):
        return_val = True

        if (len(self.results) != 33):
            tracer.error("Expected result count: 33; actual result count: {0}".format(len(self.results)))
            return_val = False

        if (self.invalid_record_count > 0):
            tracer.error("{0} invalid record(s) found".format(self.invalid_record_count))
            return_val = False

        return return_val

class TestElasticIntegration(unittest.TestCase):
    """Tests unit integration and Elastic integration
    """

    def test_multi_step_elastic_query(self):
        # Load configuration to find Elastic node
        config = None

        try:
            with open("{0}{1}elastic_test_config.json".format(os.path.dirname(__file__), os.path.sep)) as config_file:
                config = json.load(config_file)
        except IOError:
            config = None

        elastic_host = "127.0.0.1"
        elastic_port = 9001

        if (config is not None):
            try:
                elastic_host = config["host"]
            except KeyError:
                pass

            try:
                elastic_port = config["port"]
            except KeyError:
                pass

        # Generate test data
        seed = generate_data.TestCollection(0, 10000)
        piv1 = generate_data.TestCollection(1, 1000)
        join1 = generate_data.TestCollection(2, 100)

        # Load it into Elasticsearch
        data.load_data.elastic_post(seed.recs)
        data.load_data.elastic_post(piv1.recs)
        data.load_data.elastic_post(join1.recs)

        # Ugly, but need to give Elastic some time to index the data
        time.sleep(10)

        # Prepare the Elasticsearch queries for feeding JiggleQ
        q1 = ElasticsearchDataSource("jiggleqsystest_set0", "field_0=\"data_0_0\"", {"hosts":["{0}:{1}".format(elastic_host, elastic_port)]})
        q2 = ElasticsearchDataSource("jiggleqsystest_set1", "field_1=\"data_1_1\"", {"hosts":["{0}:{1}".format(elastic_host, elastic_port)]})
        q3 = ElasticsearchDataSource("jiggleqsystest_set2", "field_0=\"data_2_0\"", {"hosts":["{0}:{1}".format(elastic_host, elastic_port)]})

        r = TestResultHandler()
        s = JiggleQ(q1).pivot_to(q2, F("key_0_1") == F("key_1_1")).join_to(q3, (F("key_1_0") == F("key_2_0")) & (F("key_1_1") == F("key_2_1")) & (F("key_1_2") == F("key_2_2")), field="joined_rec", array=True, exclude_empty_joins=True)
        s.result_handler(r)

        t_start = time.time()
        s.execute(stream=True)
        t_end = time.time()
        elapsed_time = round(t_end - t_start, 1)

        self.assertTrue(r.validate())
    
