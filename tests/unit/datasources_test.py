# -*- coding: utf-8 -*-

"""@package datasources_test
Tests for jiggleq.datasources
"""

import unittest
import tempfile
import json
import os
import types

from jiggleq.datasources import AppDataSourceBuilder, JsonLinesDataSource, JsonDataSource, CsvDataSource, ElasticsearchDataSource
from jiggleq import jqexception

class TestConfig(unittest.TestCase):
    """Tests Config class
    """

    def test_data_source_introspection(self):
        """Data sources are correctly discovered and indexed.
        """
        expected_data_sources = ["json_lines", "jsl", "json", "js", "elasticsearch", "el", "csv"]
        expected_data_sources.sort()

        subject = AppDataSourceBuilder({})
        self.assertEquals(len(subject._source_type_mappings), len(expected_data_sources))
        self.assertEquals(subject._source_type_mappings["json_lines"], JsonLinesDataSource)
        self.assertEquals(subject._source_type_mappings["jsl"], JsonLinesDataSource)
        self.assertEquals(subject._source_type_mappings["json"], JsonDataSource)
        self.assertEquals(subject._source_type_mappings["js"], JsonDataSource)
        self.assertEquals(subject._source_type_mappings["csv"], CsvDataSource)

        self.assertEquals(subject.valid_source_types, ", ".join(expected_data_sources))

    def test_uri_parse_valid(self):
        """Parse a correctly formatted source type out of a URI string.
        """
        subject = AppDataSourceBuilder({})
        self.assertEquals(subject._parse_uri("json_lines:/test/uri"), {"source_type":"json_lines", "uri":"/test/uri", "data_source_class":JsonLinesDataSource})

    def test_uri_parse_case_insensitive_source_type(self):
        """Parse a correctly formatted source type out of a URI string, regardless of source type case.
        """
        subject = AppDataSourceBuilder({})
        self.assertEquals(subject._parse_uri("JSON_lines:/test/uri"), {"source_type":"json_lines", "uri":"/test/uri", "data_source_class":JsonLinesDataSource})

    def test_uri_parse_alternative_source_type_ident(self):
        """Parse a correctly formatted source type out of a URI string, using an alternative source type ident string.
        """
        subject = AppDataSourceBuilder({})
        self.assertEquals(subject._parse_uri("jsl:/test/uri"), {"source_type":"json_lines", "uri":"/test/uri", "data_source_class":JsonLinesDataSource})

    def test_parse_uri_invalid(self):
        """Parse a missing source type from a URI string
        """
        subject = AppDataSourceBuilder({})
        with self.assertRaises(jqexception.DataSourceBuildError):
            subject._parse_uri("/test/uri")

    def test_parse_uri_not_greedy(self):
        """Make sure the source type parsing finishes at the first colon
        """
        subject = AppDataSourceBuilder({})
        self.assertEquals(subject._parse_uri("json_lines:not_a_type:/test/uri"), {"source_type":"json_lines", "uri":"not_a_type:/test/uri", "data_source_class":JsonLinesDataSource})

    def test_parse_uri_invalid_source_type(self):
        """Parse an invalid source type from a URI string
        """
        subject = AppDataSourceBuilder({})
        with self.assertRaises(jqexception.DataSourceBuildError):
            subject._parse_uri("invalid_source_type:/test/uri")

    def test_datasource_construction(self):
        """Construct a DataSource object correctly from a URI and filter string.
        """
        subject = AppDataSourceBuilder({"data_sources":{"elasticsearch":{"hosts":["127.0.0.1:5601"]}}})
        constructed_datasource = subject("elasticsearch:test_index_name", "test_filter_string")
        self.assertTrue(isinstance(constructed_datasource, ElasticsearchDataSource))
        self.assertEquals(constructed_datasource.index_name, "test_index_name")
        self.assertEquals(constructed_datasource.filter_string, "test_filter_string")


    def test_elasticds_no_hosts_config(self):
        """Elasticsearch datasource is not configured with hosts.
        """
        with self.assertRaises(jqexception.DataSourceBuildError):
            subject = AppDataSourceBuilder({"data_sources":{"elasticsearch":{}}})("elasticsearch:test_index_name", "test_filter_string")

    def test_elasticds_default_config(self):
        """Elasticsearch datasource config items are correctly set to defaults.
        """
        subject = AppDataSourceBuilder({"data_sources":{"elasticsearch":{"hosts":["127.0.0.1:5601"]}}})("elasticsearch:test_index_name", "test_filter_string")
        self.assertEquals(subject.config["hosts"], ["127.0.0.1:5601"])
        self.assertEquals(subject.config["timeout"], 10)
        self.assertEquals(subject.config["use_ssl"], False)
        self.assertEquals(subject.config["verify_certs"], False)
        self.assertEquals(subject.config["ca_certs"], None)
        self.assertEquals(subject.config["client_cert"], None)
        self.assertEquals(subject.config["client_key"], None)

    def test_elasticds_supplied_config(self):
        """Elasticsearch datasource config applied correctly
        """
        subject = AppDataSourceBuilder({"data_sources":{"elasticsearch":{"hosts":["127.0.0.1:5601","10.10.10.1:1280"],"timeout":20,"use_ssl":True,"verify_certs":True,"ca_certs":"/tmp/ca_certs","client_cert":"/tmp/client_cert","client_key":"/tmp/client_key"}}})("elasticsearch:test_index_name", "test_filter_string")
        self.assertEquals(subject.config["hosts"], ["127.0.0.1:5601","10.10.10.1:1280"])
        self.assertEquals(subject.config["timeout"], 20)
        self.assertEquals(subject.config["use_ssl"], True)
        self.assertEquals(subject.config["verify_certs"], True)
        self.assertEquals(subject.config["ca_certs"], "/tmp/ca_certs")
        self.assertEquals(subject.config["client_cert"], "/tmp/client_cert")
        self.assertEquals(subject.config["client_key"], "/tmp/client_key")

    def test_json_lines_batch_load(self):
        """json_lines data source batch loads a file successfully.
        """

        test_data = '{"test_field_1a":"test_value_1a", "test_field_2a":"test_value_2a"}\n{"test_field_1b":"test_value_1b", "test_field_2b":"test_value_2b"}\n{"test_field_1c":"test_value_1c", "test_field_2c":"test_value_2c"}'
        tmpfile = tempfile.mkstemp()
        with open(tmpfile[1], "w") as config_file:
            config_file.write(test_data)

        try:
            subject = JsonLinesDataSource(tmpfile[1], None)
            self.assertEquals(subject.batch(), [{"test_field_1a":"test_value_1a", "test_field_2a":"test_value_2a"},{"test_field_1b":"test_value_1b", "test_field_2b":"test_value_2b"},{"test_field_1c":"test_value_1c", "test_field_2c":"test_value_2c"}])
        finally:
            os.close(tmpfile[0])
            os.unlink(tmpfile[1])

    def test_json_lines_stream_load(self):
        """json_lines data source stream loads a file successfully.
        """

        test_data = '{"test_field_1a":"test_value_1a", "test_field_2a":"test_value_2a"}\n{"test_field_1b":"test_value_1b", "test_field_2b":"test_value_2b"}\n{"test_field_1c":"test_value_1c", "test_field_2c":"test_value_2c"}'
        tmpfile = tempfile.mkstemp()
        with open(tmpfile[1], "w") as config_file:
            config_file.write(test_data)

        try:
            result = []
            subject = JsonLinesDataSource(tmpfile[1], None)
            for record in subject.stream():
                result.append(record)
            self.assertEquals(result, [{"test_field_1a":"test_value_1a", "test_field_2a":"test_value_2a"},{"test_field_1b":"test_value_1b", "test_field_2b":"test_value_2b"},{"test_field_1c":"test_value_1c", "test_field_2c":"test_value_2c"}])
        finally:
            os.close(tmpfile[0])
            os.unlink(tmpfile[1])

    def test_json_lines_load_error(self):
        """json_lines data source encounters error loading file.
        """

        test_data = '{"test_field_1a":"test_value_1a", "test_field_2a":"test_value_2a"}\n{"test_field_1b":"test_value_1b", "test_field_2b":"test_value_2b"}\n{"test_field_1c":"test_value_1c", "test_field_2c":"test_value_2c"'
        tmpfile = tempfile.mkstemp()
        with open(tmpfile[1], "w") as config_file:
            config_file.write(test_data)

        try:
            result = []
            subject = JsonLinesDataSource(tmpfile[1], None)
            with self.assertRaises(Exception):
                subject.batch()
        finally:
            os.close(tmpfile[0])
            os.unlink(tmpfile[1])

    def test_json_lines_utf8(self):
        """json_lines data source handles UTF-8 OK.
        """

        test_data = u'{"กว่า":"κόσμε", "test_field_2a":"test_value_2a"}\n{"test_field_1b":"いろはにほへとちりぬるを", "Heizölrückstoßabdämpfung":"test_value_2b"}\n{"test_field_1c":"test_value_1c", "test_field_2c":"test_value_2c"}'.encode("utf-8")
        tmpfile = tempfile.mkstemp()
        with open(tmpfile[1], "w") as config_file:
            config_file.write(test_data)

        try:
            subject = JsonLinesDataSource(tmpfile[1], None)
            self.assertEquals(subject.batch(), [{u"กว่า":u"κόσμε", "test_field_2a":"test_value_2a"},{"test_field_1b":u"いろはにほへとちりぬるを", u"Heizölrückstoßabdämpfung":"test_value_2b"},{"test_field_1c":"test_value_1c", "test_field_2c":"test_value_2c"}])
        finally:
            os.close(tmpfile[0])
            os.unlink(tmpfile[1])

    def test_json_batch_load(self):
        """json data source batch loads a file successfully.
        """
        test_data = '[{"test_field_1a":"test_value_1a", "test_field_2a":"test_value_2a"},{"test_field_1b":"test_value_1b", "test_field_2b":"test_value_2b"},{"test_field_1c":"test_value_1c", "test_field_2c":"test_value_2c"}]'
        tmpfile = tempfile.mkstemp()
        with open(tmpfile[1], "w") as config_file:
            config_file.write(test_data)

        try:
            subject = JsonDataSource(tmpfile[1], None)
            self.assertEquals(subject.batch(), [{"test_field_1a":"test_value_1a", "test_field_2a":"test_value_2a"},{"test_field_1b":"test_value_1b", "test_field_2b":"test_value_2b"},{"test_field_1c":"test_value_1c", "test_field_2c":"test_value_2c"}])
        finally:
            os.close(tmpfile[0])
            os.unlink(tmpfile[1])

    def test_json_stream_load(self):
        """json data source stream loads a file successfully.
        """
        test_data = '[{"test_field_1a":"test_value_1a", "test_field_2a":"test_value_2a"},{"test_field_1b":"test_value_1b", "test_field_2b":"test_value_2b"},{"test_field_1c":"test_value_1c", "test_field_2c":"test_value_2c"}]'
        tmpfile = tempfile.mkstemp()
        with open(tmpfile[1], "w") as config_file:
            config_file.write(test_data)

        try:
            subject = JsonDataSource(tmpfile[1], None)
            result = []
            for record in subject.stream():
                result.append(record)

            self.assertEquals(result, [{"test_field_1a":"test_value_1a", "test_field_2a":"test_value_2a"},{"test_field_1b":"test_value_1b", "test_field_2b":"test_value_2b"},{"test_field_1c":"test_value_1c", "test_field_2c":"test_value_2c"}])
        finally:
            os.close(tmpfile[0])
            os.unlink(tmpfile[1])

    def test_json_no_list_root(self):
        """json data source stream cannot load a document without a list root.
        """
        test_data = '{"a":{"test_field_1a":"test_value_1a", "test_field_2a":"test_value_2a"},"b":{"test_field_1b":"test_value_1b", "test_field_2b":"test_value_2b"},"c":{"test_field_1c":"test_value_1c", "test_field_2c":"test_value_2c"}}'
        tmpfile = tempfile.mkstemp()
        with open(tmpfile[1], "w") as config_file:
            config_file.write(test_data)

        try:
            subject = JsonDataSource(tmpfile[1], None)
            with self.assertRaises(jqexception.DataSourceError):
                subject.batch()
                
        finally:
            os.close(tmpfile[0])
            os.unlink(tmpfile[1])

    def test_json_utf8(self):
        """json data source handles UTF-8 OK.
        """
        test_data = u'[{"กว่า":"κόσμε", "test_field_2a":"test_value_2a"},{"test_field_1b":"いろはにほへとちりぬるを", "Heizölrückstoßabdämpfung":"test_value_2b"},{"test_field_1c":"test_value_1c", "test_field_2c":"test_value_2c"}]'.encode("utf-8")
        tmpfile = tempfile.mkstemp()
        with open(tmpfile[1], "w") as config_file:
            config_file.write(test_data)

        try:
            subject = JsonDataSource(tmpfile[1], None)
            self.assertEquals(subject.batch(), [{u"กว่า":u"κόσμε", "test_field_2a":"test_value_2a"},{"test_field_1b":u"いろはにほへとちりぬるを", u"Heizölrückstoßabdämpfung":"test_value_2b"},{"test_field_1c":"test_value_1c", "test_field_2c":"test_value_2c"}])
        finally:
            os.close(tmpfile[0])
            os.unlink(tmpfile[1])

    def test_csv_batch_load(self):
        """csv data source batch loads a file successfully with field names.
        """
        test_data = '"field_a","field_b","field_c"\n"row0cola","row0colb","row0colc"\n"row1cola","row1colb","row1colc"\n"row2cola","row2colb","row2colc"\n'
        tmpfile = tempfile.mkstemp()
        with open(tmpfile[1], "w") as config_file:
            config_file.write(test_data)

        try:
            subject = CsvDataSource(tmpfile[1], None, {"first_row_contains_field_names":True})
            self.assertEquals(subject.batch(), [{"field_a":"row0cola", "field_b":"row0colb", "field_c":"row0colc"},{"field_a":"row1cola", "field_b":"row1colb", "field_c":"row1colc"},{"field_a":"row2cola", "field_b":"row2colb", "field_c":"row2colc"}])
        finally:
            os.close(tmpfile[0])
            os.unlink(tmpfile[1])

    def test_csv_stream_load(self):
        """csv data source stream loads a file successfully with field names.
        """
        test_data = '"field_a","field_b","field_c"\n"row0cola","row0colb","row0colc"\n"row1cola","row1colb","row1colc"\n"row2cola","row2colb","row2colc"\n'
        tmpfile = tempfile.mkstemp()
        with open(tmpfile[1], "w") as config_file:
            config_file.write(test_data)

        try:
            subject = CsvDataSource(tmpfile[1], None, {"first_row_contains_field_names":True})
            result = []
            for record in subject.stream():
                result.append(record)

            self.assertEquals(result, [{"field_a":"row0cola", "field_b":"row0colb", "field_c":"row0colc"},{"field_a":"row1cola", "field_b":"row1colb", "field_c":"row1colc"},{"field_a":"row2cola", "field_b":"row2colb", "field_c":"row2colc"}])
        finally:
            os.close(tmpfile[0])
            os.unlink(tmpfile[1])

    def test_csv_no_field_names(self):
        """csv data source loads a file with no field names.
        """
        test_data = '"row0cola","row0colb","row0colc"\n"row1cola","row1colb","row1colc"\n"row2cola","row2colb","row2colc"\n'
        tmpfile = tempfile.mkstemp()
        with open(tmpfile[1], "w") as config_file:
            config_file.write(test_data)

        try:
            subject = CsvDataSource(tmpfile[1], None, {"first_row_contains_field_names":False})
            self.assertEquals(subject.batch(), [{"column_1":"row0cola", "column_2":"row0colb", "column_3":"row0colc"},{"column_1":"row1cola", "column_2":"row1colb", "column_3":"row1colc"},{"column_1":"row2cola", "column_2":"row2colb", "column_3":"row2colc"}])
        finally:
            os.close(tmpfile[0])
            os.unlink(tmpfile[1])

    def test_csv_utf8(self):
        """csv input with UTF-8 characters
        """
        test_data = u'"กว่า","áðan","τὴν"\n"κόσμε","row0colb","row0colc"\n"row1cola","Heizölrückstoßabdämpfung","row1colc"\n"row2cola","row2colb","いろはにほへとちりぬるを"\n'.encode("utf-8")
        tmpfile = tempfile.mkstemp()
        with open(tmpfile[1], "w") as config_file:
            config_file.write(test_data)

        try:
            subject = CsvDataSource(tmpfile[1], None, {"first_row_contains_field_names":True})
            self.assertEquals(subject.batch(), [{u"กว่า":u"κόσμε", u"áðan":"row0colb", u"τὴν":"row0colc"},{u"กว่า":"row1cola", u"áðan":u"Heizölrückstoßabdämpfung", u"τὴν":"row1colc"},{u"กว่า":"row2cola", u"áðan":"row2colb", u"τὴν":u"いろはにほへとちりぬるを"}])
        finally:
            os.close(tmpfile[0])
            os.unlink(tmpfile[1])

