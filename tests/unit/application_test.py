"""@package application_test
Tests for weaveq.application
"""

import unittest
import tempfile
import json
import os
import types

from weaveq.application import Config, App
from weaveq import wqexception

class TestConfig(unittest.TestCase):
    """Tests Config class
    """

    def test_validate_item_single_path_element(self):
        """A single existing path element passed to item validator
        """
        subject = Config()
        subject._validate_item({"test_el":"test_val"}, "test_el", str)
        self.assertEquals(len(subject.config), 0)

    def test_validate_item_multi_path_element(self):
        """Multiple existing path elements passed to item validator
        """
        subject = Config()
        subject._validate_item({"test_el":{"test_el2":{"test_el3":"test_val"}}}, "test_el/test_el2/test_el3", str)
        self.assertEquals(len(subject.config), 0)

    def test_validate_item_absent_multi_path_element(self):
        """Multiple path elements passed to item validator, one missing
        """
        subject = Config()
        with self.assertRaises(wqexception.ConfigurationError):
            subject._validate_item({"test_el":{"test_el2":{"test_elc":"test_val"}}}, "test_el/test_el2/test_el3", str)
            
        with self.assertRaises(wqexception.ConfigurationError):
            subject._validate_item({"test_el":{"test_elb":{"test_el3":"test_val"}}}, "test_el/test_el2/test_el3", str)

        with self.assertRaises(wqexception.ConfigurationError):
            subject._validate_item({"test_e":{"test_el2":{"test_el3":"test_val"}}}, "test_el/test_el2/test_el3", str)

    def test_validate_item_wrong_type(self):
        """Item is wrong type
        """
        subject = Config()
        with self.assertRaises(wqexception.ConfigurationError):
            subject._validate_item({"test_el":{"test_el2":{"test_el3":"test_val"}}}, "test_el/test_el2/test_el3", int)

    def test_validate_item_less_than_min_len(self):
        """Item contains too few elements
        """
        subject = Config()
        with self.assertRaises(wqexception.ConfigurationError):
            subject._validate_item({"test_el":{"test_el2":{"test_el3":[1]}}}, "test_el/test_el2/test_el3", list, 2)

    def test_validate_item_more_than_max_len(self):
        """Item contains too many elements
        """
        subject = Config()
        with self.assertRaises(wqexception.ConfigurationError):
            subject._validate_item({"test_el":{"test_el2":{"test_el3":[1,2]}}}, "test_el/test_el2/test_el3", list, 1, 1)

    def test_validate_item_len_in_range(self):
        """Item contains elements within specified range
        """
        subject = Config()
        subject._validate_item({"test_el":{"test_el2":{"test_el3":[1,2]}}}, "test_el/test_el2/test_el3", list, 1, 4)

    def test_validate_item_len_on_range_boundary(self):
        """Item contains elements within specified range
        """
        subject = Config()
        subject._validate_item({"test_el":{"test_el2":{"test_el3":[1,2]}}}, "test_el/test_el2/test_el3", list, 2, 2)

    def test_config_root_bad_type(self):
        """Correct root element, but not dict
        """
        subject = Config()
        with self.assertRaises(wqexception.ConfigurationError):
            subject.apply_config({"data_sources":""})

    def test_config_elk_bad_type(self):
        """Elastic data source, but not dict
        """
        subject = Config()
        with self.assertRaises(wqexception.ConfigurationError):
            subject.apply_config({"data_sources":{"elasticsearch":""}})

    def test_config_elk_hosts_bad_type(self):
        """Elastic hosts but not list
        """
        subject = Config()
        with self.assertRaises(wqexception.ConfigurationError):
            subject.apply_config({"data_sources":{"elasticsearch":{"hosts":""}}})

    def test_config_elk_host_el_bad_type(self):
        """Elastic hosts with non-str host
        """
        subject = Config()
        with self.assertRaises(wqexception.ConfigurationError):
            subject.apply_config({"data_sources":{"elasticsearch":["testhost",1,"testhost2"]}})

    def test_config_min_valid(self):
        """Minimum valid config
        """
        subject = Config()
        subject.apply_config({"data_sources":{"elasticsearch":{"hosts":["test1","test2"]},"csv":{"first_row_names":True}}})
        self.assertEquals(subject.config, {"data_sources":{"elasticsearch":{"hosts":["test1","test2"], "timeout":10, "use_ssl":False, "verify_certs":False, "ca_certs":None, "client_cert":None, "client_key":None},"csv":{"first_row_names":True}}})

    def test_config_default_overrides(self):
        """Config with default field values overridden
        """
        subject = Config()
        subject.apply_config({"data_sources":{"elasticsearch":{"hosts":["test1","test2"], "timeout":20, "use_ssl":True, "verify_certs":True, "ca_certs":"/test/ca/cert", "client_cert":"/test/client/cert", "client_key":"/test/client/key"},"csv":{"first_row_names":True}}})
        self.assertEquals(subject.config, {"data_sources":{"elasticsearch":{"hosts":["test1","test2"], "timeout":20, "use_ssl":True, "verify_certs":True, "ca_certs":"/test/ca/cert", "client_cert":"/test/client/cert", "client_key":"/test/client/key"},"csv":{"first_row_names":True}}})

    def test_config_file(self):
        """Minimum valid config from file
        """
        tmpfile = tempfile.mkstemp()
        json_config = json.dumps({"data_sources":{"elasticsearch":{"hosts":["test1","test2"], "timeout":20, "use_ssl":True, "verify_certs":True, "ca_certs":"/test/ca/cert", "client_cert":"/test/client/cert", "client_key":"/test/client/key"},"csv":{"first_row_names":False}}})

        with open(tmpfile[1], "w") as config_file:
            config_file.write(json_config)

        try:
            subject = Config(tmpfile[1])
            self.assertEquals(subject.config, {u"data_sources":{u"elasticsearch":{u"hosts":[u"test1",u"test2"], u"timeout":20, u"use_ssl":True, u"verify_certs":True, u"ca_certs":u"/test/ca/cert", u"client_cert":u"/test/client/cert", u"client_key":u"/test/client/key"},"csv":{"first_row_names":False}}})
        finally:
            os.close(tmpfile[0])
            os.unlink(tmpfile[1])
            
class TestApp(unittest.TestCase):
    """Tests App class.
    """

    def setUp(self):
        self._config_file = tempfile.mkstemp()
        self._mock_stdin = tempfile.mkstemp()
        self._mock_stdout = tempfile.mkstemp()

    def tearDown(self):
        os.close(self._config_file[0])
        os.unlink(self._config_file[1])

        os.close(self._mock_stdin[0])
        os.unlink(self._mock_stdin[1])

        os.close(self._mock_stdout[0])
        os.unlink(self._mock_stdout[1])

    def test_config_failure(self):
        with open(self._config_file[1], "w") as config_file:
            config_file.write("{}")

        with self.assertRaises(wqexception.ConfigurationError):
            subject = App(mock_args=["-c", self._config_file[1], "-q", "dummy query"])

    def test_config_valid_explicit_query_string(self):
        with open(self._config_file[1], "w") as config_file:
            config_file.write('{"data_sources":{"elasticsearch":{"hosts":["test1","test2"]},"csv":{"first_row_names":true}}}')

        subject = App(mock_args=["-c", self._config_file[1], "-q", "placeholder_query_string"])
        self.assertEquals(subject._config, {"data_sources":{"elasticsearch":{"hosts":["test1","test2"], "timeout":10, "use_ssl":False, "verify_certs":False, "ca_certs":None, "client_cert":None, "client_key":None},"csv":{"first_row_names":True}}})
        self.assertEquals(subject._query_string, "placeholder_query_string")
        self.assertEquals(subject._output_file, subject._stdout)

    def test_output_file_omitted(self):
        with open(self._config_file[1], "w") as config_file:
            config_file.write('{"data_sources":{"elasticsearch":{"hosts":["test1","test2"]},"csv":{"first_row_names":true}}}')

        subject = App(mock_args=["-c", self._config_file[1], "-q", "placeholder_query_string", "-o", self._mock_stdout[1]])
        self.assertEquals(subject._config, {"data_sources":{"elasticsearch":{"hosts":["test1","test2"], "timeout":10, "use_ssl":False, "verify_certs":False, "ca_certs":None, "client_cert":None, "client_key":None},"csv":{"first_row_names":True}}})
        self.assertEquals(subject._query_string, "placeholder_query_string")
        self.assertNotEquals(subject._output_file, subject._stdout)



