# -*- coding: utf-8 -*-

"""!
@package weaveq.application Application entry point.
"""

# Copyright 2017 James Mistry.
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), 
# to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, 
# and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER 
# DEALINGS IN THE SOFTWARE.

from __future__ import print_function, absolute_import
import json
import argparse
import types
import sys
import six

import weaveq.build_constants
import weaveq.wqexception
import weaveq.parser
import weaveq.query
import weaveq.datasources

class FileOutputResultHandler(weaveq.query.ResultHandler):
    def __init__(self, file_object):
        """!
        Constructor.

        @param file_object string: open file object to which results should be written
        """
        self._destination = file_object
    
    def __call__(self, result, handler_output):
        print(json.dumps(result), file=self._destination)

    def success(self):
        return True

class Config(object):
    """!
    Loads, parses and validates an application configuration.
    """

    @staticmethod
    def default_config():
        return {"data_sources":{"elasticsearch":None,"csv":{"first_row_contains_field_names":True}}}

    def __init__(self, config_filename = None):
        """!
        Constructor. Loads and validates a configuration file, if specified.
        """
        self.config = {}

        config_data = {}

        if (config_filename is not None):
            try:
                with open(config_filename) as config_file:
                    config_data = json.load(config_file)
            except (OSError, IOError) as e:
                raise weaveq.wqexception.ConfigurationError("Problem reading file: {0}".format(str(e)))

            self.apply_config(config_data)

    def _validate_item(self, config_data, item_path, required_type, min_len = None, max_len = None):
        cur_obj = config_data
        path_elements = item_path.split("/")
        path_so_far = ""
        for el in path_elements:
            path_so_far = "{0}{1}{2}".format(path_so_far, "/" if (len(path_so_far) > 0) else "", el)
            if (el not in cur_obj):
                raise weaveq.wqexception.ConfigurationError("Missing '{0}' configuration item (configuration file format is documented at {1})".format(path_so_far, weaveq.build_constants.config_doc_url))
            
            cur_obj = cur_obj[el]

        if (not isinstance(cur_obj, required_type)):
            raise weaveq.wqexception.ConfigurationError("'{0}' configuration item must be of type {1} (configuration file format is documented at {2})".format(item_path, str(required_type), weaveq.build_constants.config_doc_url))

        if (min_len is not None):
            if (len(cur_obj) < min_len):
                raise weaveq.wqexception.ConfigurationError("'{0}' configuration item must contain a minimum of {1} element(s), but there are {2} specified (configuration file format is documented at {3})".format(item_path, min_len, len(cur_obj), weaveq.build_constants.config_doc_url))

        if (max_len is not None):
            if (len(cur_obj) > max_len):
                raise weaveq.wqexception.ConfigurationError("'{0}' configuration item must contain a maximum of {1} element(s), but there are {2} specified (configuration file format is documented at {3})".format(item_path, max_len, len(cur_obj), weaveq.build_constants.config_doc_url))

    def apply_config(self, config_data):
        self._validate_item(config_data, "data_sources", dict, 2, 2)
        self._validate_item(config_data, "data_sources/elasticsearch", dict)
        self._validate_item(config_data, "data_sources/elasticsearch/hosts", list, 1)

        for host in config_data["data_sources"]["elasticsearch"]["hosts"]:
            if (not isinstance(host, six.string_types)):
                raise weaveq.wqexception.ConfigurationError("Invalid Elasticsearch host specified: host items must be strings, not {0}".format(str(type(host))))

        if ("timeout" not in config_data["data_sources"]["elasticsearch"]):
            config_data["data_sources"]["elasticsearch"]["timeout"] = 10
        else:
            self._validate_item(config_data, "data_sources/elasticsearch/timeout", int)

        if ("use_ssl" not in config_data["data_sources"]["elasticsearch"]):
            config_data["data_sources"]["elasticsearch"]["use_ssl"] = False
        else:
            self._validate_item(config_data, "data_sources/elasticsearch/use_ssl", bool)

        if ("verify_certs" not in config_data["data_sources"]["elasticsearch"]):
            config_data["data_sources"]["elasticsearch"]["verify_certs"] = False
        else:
            self._validate_item(config_data, "data_sources/elasticsearch/verify_certs", bool)

        if ("ca_certs" not in config_data["data_sources"]["elasticsearch"]):
            config_data["data_sources"]["elasticsearch"]["ca_certs"] = None
        else:
            self._validate_item(config_data, "data_sources/elasticsearch/ca_certs", six.string_types)

        if ("client_cert" not in config_data["data_sources"]["elasticsearch"]):
            config_data["data_sources"]["elasticsearch"]["client_cert"] = None
        else:
            self._validate_item(config_data, "data_sources/elasticsearch/client_cert", six.string_types)

        if ("client_key" not in config_data["data_sources"]["elasticsearch"]):
            config_data["data_sources"]["elasticsearch"]["client_key"] = None
        else:
            self._validate_item(config_data, "data_sources/elasticsearch/client_key", six.string_types)

        self._validate_item(config_data, "data_sources/csv", dict)
        self._validate_item(config_data, "data_sources/csv/first_row_contains_field_names", bool)

        self.config = config_data

class App(object):
    """!
    Application entry point and global state.
    """

    def __init__(self, mock_args = None, mock_stdin = None, mock_stdout = None):
        """!
        Constructor. Parses and stores application arguments, and loads the supplied configuration file.
        """
        self._output_file = None

        if (mock_stdin is not None):
            self._stdin = open(mock_stdin)
        else:
            self._stdin = sys.stdin

        if (mock_stdout is not None):
            self._stdout = open(mock_stdout)
        else:
            self._stdout = sys.stdout

        arg_parser = argparse.ArgumentParser(prog="weaveq", description="Runs pivot and join queries across collections of data with support for various data sources, including Elasticsearch and JSON")
        arg_parser.add_argument("-c", "--config", help="path to the configuration file. Required if using an Elasticsearch data source. Its format is documented at {0}".format(weaveq.build_constants.config_doc_url), required=False)
        arg_parser.add_argument("-q", "--query", help="query string to be executed", required=True)
        arg_parser.add_argument("-o", "--output", help="path to the output file containing line-delimitted JSON query results. Omit this argument or specify - (dash) to write to stdout", required=False)
        arg_parser.add_argument("--version", action="version", version="WeaveQ {0}.{1}.{2}-{3}".format(weaveq.build_constants.major_version, weaveq.build_constants.minor_version, weaveq.build_constants.release_version, weaveq.build_constants.release_phase))
        
        self._args = {}
        if (mock_args is None):
            self._args = vars(arg_parser.parse_args())
        else:
            self._args = vars(arg_parser.parse_args(mock_args))

        self._config = None
        if (self._args["config"] is None):
            self._config = Config.default_config()
        else:
            try:
                config_loader = Config(self._args["config"])
                self._config = config_loader.config
            except Exception as e:
                print("Couldn't load configuration file. {1}".format(self._args["config"], str(e)), file=sys.stderr)
                raise

        if (self._args["output"] is not None):
            if (self._args["output"] == "-"):
                self._output_file = self._stdout
            else:
                try:
                    self._output_file = open(self._args["output"], "w")
                except Exception as e:
                    print("Couldn't open file '{0}': {1}".format(self._output_file, str(e)), file=sys.stderr)
                    raise
        else:
            self._output_file = self._stdout

        self._query_string = self._args["query"]

    def __del__(self):
        if (self._output_file is not None):
            self._output_file.close()

        if (self._stdin is not None):
            self._stdin.close()

        if (self._stdout is not None):
            self._stdout.close()

    def run(self):
        builder = weaveq.datasources.AppDataSourceBuilder(self._config)
        query_compiler = weaveq.parser.TextQuery(builder)

        try:
            compiled_query = query_compiler.compile_query(self._query_string)
        except Exception as e:
            print("Error compiling query. {0}".format(str(e)), file=sys.stderr)
            raise

        result_handler = FileOutputResultHandler(self._output_file)
        compiled_query.result_handler(result_handler)

        try:
            compiled_query.execute(stream=True)
        except Exception as e:
            print("Error running query. {0}".format(str(e)), file=sys.stderr)
            raise

