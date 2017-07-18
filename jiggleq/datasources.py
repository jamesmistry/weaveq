# -*- coding: utf-8 -*-

"""!
@package jiggleq.datasources DataSource classes and factory for use with the query parser.
"""

import inspect
import sys
import abc
import json
import csv
import collections
import elasticsearch
import elasticsearch_dsl

import parser
import query
import jqexception

class DiscoverableDataSource(object):
    """!
    Superclass of all classes that can be used to instantiate supported query.DataSource objects.
    """
    pass

class JsonLinesDataSource(query.DataSource, DiscoverableDataSource):
    """!
    @brief Data source for files containing records in "JSON lines" format.

    JSON lines consists of line-delimitted JSON documents that are to be treated as separate from one another. These documents may not contain newline sequences (these should be escaped if required).
    """

    def __init__(self, filename, filter_string, config = None):
        """!
        Constructor.

        @param filename string: path to the JSON lines file from which to read the data
        @param filter_string string: not applicable to this data source - must be @c None or an exception will be raised
        @param config dict: not applicable to this data source - must be @c None or an exception will be raised
        """
        # Only call the query.DataSource constructor
        super(JsonLinesDataSource, self).__init__(filename, filter_string)

        if (config is not None):
            raise jqexception.DataSourceBuildError("The json_lines data source type does not currently support any configuration parameters.")

        if (filter_string is not None):
            raise jqexception.DataSourceBuildError("The json_lines data source type does not currently support the #filter statement.")

        ## @var filename
        # Filename of the data source file
        self.filename = filename

    @staticmethod
    def string_idents():
        """!
        Provides strings that are to be used to identify this data source.

        @return a list containing the strings that may identify this data source
        """
        return ["json_lines", "jsl"]

    def _load_json_lines(self):
        with open(self.filename) as json_file:
            for json_line in json_file:
                json_record = json.loads(json_line, object_pairs_hook=collections.OrderedDict)
                yield json_record

    def batch(self):
        """!
        @see jiggleq.query.DataSource
        """
        return_val = []

        for record in self._load_json_lines():
            return_val.append(record)

        return return_val

    def stream(self):
        """!
        @see jigglq.query.DataSource
        """
        for record in self._load_json_lines():
            yield record

class JsonDataSource(query.DataSource, DiscoverableDataSource):
    """!
    @brief Data source for files containing records in JSON format.

    JSON files must consist of a root-level array in object hierarchy which contains JSON objects. Non-object elements of the array are silently ignored by this data source.
    """

    def __init__(self, filename, filter_string, config = None):
        """!
        Constructor.

        @param filename string: path to the JSON file from which to read the data
        @param filter_string string: not applicable to this data source - must be @c None or an exception will be raised
        @param config dict: not applicable to this data source - must be @c None or an exception will be raised
        """
        # Only call the query.DataSource constructor
        super(JsonDataSource, self).__init__(filename, filter_string)

        if (config is not None):
            raise jqexception.DataSourceBuildError("The json data source type does not currently support any configuration parameters.")

        if (filter_string is not None):
            raise jqexception.DataSourceBuildError("The json data source type does not currently support the #filter statement.")

        ## @var filename
        # Filename of the data source file
        self.filename = filename

    @staticmethod
    def string_idents():
        """!
        Provides strings that are to be used to identify this data source.

        @return a list containing the strings that may identify this data source
        """
        return ["json", "js"]

    def _load_json(self):
        json_doc = None
        with open(self.filename) as json_file:
            json_doc = json.load(json_file, object_pairs_hook=collections.OrderedDict)

        if (not isinstance(json_doc, list)):
            raise jqexception.DataSourceError("The json data source requires that JSON documents contain lists as their root elements")

        return json_doc

    def batch(self):
        """!
        @see jiggleq.query.DataSource
        """
        return self._load_json()

    def stream(self):
        """!
        @see jiggleq.query.DataSource
        """
        json_doc = self._load_json()
        for el in json_doc:
            if (isinstance(el, object)):
                yield el

class CsvDataSource(query.DataSource, DiscoverableDataSource):
    """!
    @brief Data source for files containing records in CSV format.

    CSV files must contain line-delimitted rows consisting of comma-separated cells which either (a) must not contain double-quote characters or (b) must enclose cell values in double-quote characters and escape double-quote characters contained within the cells with a sequence consisting of two double-quotes. All CSV data must be valid UTF-8.

    Objects of this class can be configured to use the first row of a CSV document to define the names of the fields in resulting objects produced by the data source. If not configured this way, the field names are generated automatically in the form column_N where N is the 1-based index of the column of the CSV cell from which the field was generated.
    """
    def __init__(self, filename, filter_string, config = None):
        """!
        Constructor.

        @param filename string: path to the CSV file from which to read the data
        @param filter_string string: not applicable to this data source - must be @c None or an exception will be raised
        @param config dict: a dictionary containing a single boolean element called first_row_contains_field_names that determines whether or not the first CSV row should be used to define the field names of parsed objects
        """
        # Only call the query.DataSource constructor
        super(CsvDataSource, self).__init__(filename, filter_string)

        if (config is None):
            raise jqexception.DataSourceBuildError("The csv data source type requires configuration parameters to be supplied.")

        if (filter_string is not None):
            raise jqexception.DataSourceBuildError("The csv data source type does not currently support the #filter statement.")

        ## @var filename
        # Filename of the data source file
        self.filename = filename

        ## @var first_row_field_names
        # Does the first row of the CSV file contain field names?
        self.first_row_field_names = config["first_row_contains_field_names"]

    @staticmethod
    def string_idents():
        """!
        Provides strings that are to be used to identify this data source.

        @return a list containing the strings that may identify this data source
        """
        return ["csv"]

    def _load_csv(self):
        csv_doc = None
        with open(self.filename, "rb") as csv_file:
            csv_doc = csv.reader(csv_file)
            row_index = 0
            field_names = []
            for row in csv_doc:
                if ((self.first_row_field_names) and (row_index == 0)):
                    for column in row:
                        field_names.append(unicode(column, "utf-8"))
                else:
                    column_index = 0
                    record = collections.OrderedDict()
                    for column in row:
                        column_name = None
                        if (len(field_names) >= column_index + 1):
                            column_name = field_names[column_index]
                        else:
                            column_name = u"column_{0}".format(column_index + 1)

                        record[column_name] = unicode(column, "utf-8")
                        column_index += 1

                    yield record

                row_index += 1

    def batch(self):
        """!
        @see jiggleq.query.DataSource
        """
        return_val = []

        for row in self._load_csv():
            return_val.append(row)

        return return_val

    def stream(self):
        """!
        @see jiggleq.query.DataSource
        """
        for row in self._load_csv():
            yield row

class ElasticsearchDataSource(query.DataSource, DiscoverableDataSource):
    """!
    Data source for resultsets from Elasticsearch queries expressed in Query String Query syntax.
    """
    def __init__(self, index_name, filter_string = "*", config = None):
        """!
        Constructor.

        @param index_name string: name of the Elasticsearch index to query
        @param filter_string string: Query String Query to search for using Elasticsearch
        @param config dict: an dictionary containing multiple elements to be used for configuring the connection to Elasticsearch. These are: hosts (list of strings in the form host:port), timeout (integer), use_ssl (boolean), verify_certs (boolean), ca_certs (list of strings), client_cert (string) and client_key (string)
        """
        # Only call the query.DataSource constructor
        super(ElasticsearchDataSource, self).__init__(index_name, filter_string)

        if (config is None):
            raise jqexception.DataSourceBuildError("The elasticsearch data source type requires configuration parameters to be supplied.")

        ## @var index_name
        # Name of the target Elasticsearch index
        self.index_name = index_name

        ## @var filter_string
        # Query String Query to pass to Elasticsearch
        self.filter_string = filter_string

        self.config = self._validate_config(config)

        elastic_client = elasticsearch.Elasticsearch(**self.config)
        self._elastic_source = elasticsearch_dsl.Search(using=elastic_client, index=index_name).query("query_string", query=filter_string)

    def _validate_config(self, config):
        if ("hosts" not in config):
            raise jqexception.DataSourceBuildError("'hosts' is a required element in the Elasticsearch data source configuration.")
        if ("timeout" not in config):
            config["timeout"] = 10
        if ("use_ssl" not in config):
            config["use_ssl"] = False
        if ("verify_certs" not in config):
            config["verify_certs"] = False
        if ("ca_certs" not in config):
            config["ca_certs"] = None
        if ("client_cert" not in config):
            config["client_cert"] = None
        if ("client_key" not in config):
            config["client_key"] = None

        return config

    @staticmethod
    def string_idents():
        """!
        Provides strings that are to be used to identify this data source.

        @return a list containing the strings that may identify this data source
        """
        return ["elasticsearch", "el"]

    def batch(self):
        """!
        Runs the Elasticsearch query using the Elasticsearch DSL @c execute() method, returning the results as a dict.

        @see jiggleq.query.DataSource
        """
        result_val = self._elastic_source.execute()
        return result_val.to_dict()

    def stream(self):
        """!
        Runs the Elasticsearch query using the Elasticsearch DSL @c scan() method, and therefore the scroll API, returning a generator to allow iteration over the resultset.

        @see jiggleq.query.DataSource
        """
        for hit in self._elastic_source.scan():
            yield hit.to_dict()

class AppDataSourceBuilder(parser.DataSourceBuilder):
    """!
    Builds data sources for the JiggleQ application.
    """

    def __init__(self, app_config):
        """!
        Constructor. Discovers available jiggleq.query.DataSource classes within this module using introspection to identify which classes inherit from DiscoverableDataSource. These classes are then mapped to the ident strings they supply for automatic construction of jiggleq.query.DataSource objects on demand.
        """
        self._app_config = app_config
        self._source_type_mappings = {}

        ## @var valid_source_types
        # Pretty-printed list of valid data source ident strings for use in error messages
        self.valid_source_types = ""

        ident_list = []
        # Iterate over all sub-classes of DiscoverableDataSource to discover the valid data source types, how they are to be identified by users and how to instantiate them
        for target_class_name, target_class in inspect.getmembers(sys.modules[__name__]):
            if (inspect.isclass(target_class) and issubclass(target_class, DiscoverableDataSource) and (target_class is not DiscoverableDataSource)):
                # Left with only the classes that inherit from DiscoverableDataSource
                for ident in target_class.string_idents():
                    # Source types are case insensitive
                    ident = ident.lower()
                    # Add a key-value pair to the type mappings to associate the ident with the class object
                    self._source_type_mappings[ident] = target_class
                    ident_list.append(ident)

        # Make a textual list of valid source type identifiers for presentation to users in error messages
        # Sort the recorded idents to make the production of the text easy to test
        ident_list.sort()
        self.valid_source_types = ", ".join(ident_list)

    def _parse_uri(self, source_uri):
        return_val = {"source_type":None, "uri":None, "data_source_class":None}

        type_delim = source_uri.find(":")
        if (type_delim == -1):
            raise jqexception.DataSourceBuildError("A data source type must be specified in the format '<source type>:<source URI>'. For example: json_lines:/path/to/file")
        else:
            return_val["uri"] = source_uri[type_delim+1:]
            return_val["source_type"] = source_uri[0:type_delim].lower()
            try:
                return_val["data_source_class"] = self._source_type_mappings[return_val["source_type"]]

                # Normalise the source type string to the first one in the returned list
                # This is so that config keys can be reliably mapped to ident strings
                return_val["source_type"] = return_val["data_source_class"].string_idents()[0]
            except KeyError:
                raise jqexception.DataSourceBuildError("The data source type specified, '{0}', is not valid. Valid data source types are: {1}".format(return_val["source_type"], self.valid_source_types))
            
            return return_val

    def __call__(self, source_uri, filter_string):
        """!
        Builds a data source object from the data source type specified in the supplied URI.

        @param source_uri string: the URI describing the location of the data for the data source to retrieve and the kind of data source required to retrieve it, in the form TYPE:LOCATION where TYPE is a jiggleq.query.DataSource ident string and LOCATION is a location URI in a form meaningful to the specified data source type
        @param filter_string string: a filter to be passed to the data source object, which may or may not support it

        @return the build jiggleq.query.DataSource object
        """
        source = self._parse_uri(source_uri)

        data_source_config = None
        try:
            data_source_config = self._app_config["data_sources"][source["source_type"]]
        except KeyError:
            data_source_config = None

        return source["data_source_class"](source["uri"], filter_string, data_source_config)


