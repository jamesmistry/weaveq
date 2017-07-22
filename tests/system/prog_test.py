# -*- coding: utf-8 -*-

from __future__ import print_function
import json
import os
import subprocess
import unittest
import collections
import tempfile
import six

class AppRunner(object):
    def __init__(self):
        self.results = []
        self.exit_code = None

    def run(self, query, config_file = None):
        app_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

        cmd = []
        if (config_file is None):
            cmd = ["python", os.path.join(app_path, "weaveq")]
        else:
            cmd = ["python", os.path.join(app_path, "weaveq"), "-c", config_file]

        cmd.append("-q")
        cmd.append(query)

        p = subprocess.Popen(cmd, stdin=None, stdout=subprocess.PIPE, stderr=None, universal_newlines=True)
        output = p.communicate(input=None)[0]
        for output_line in output.strip().split("\n"):
            output_line = output_line.strip()
            self.results.append(output_line)

        p.wait()
        self.exit_code = p.returncode


    def check_results(self, expected):
        success = True
        if (len(self.results) != len(expected)):
            print("Expected resultset size: {0} != actual resultset size: {1}".format(len(expected), len(self.results)))
            success = False

        # Individual JSON objects output by the program are guaranteed to have ordered fields through use of OrderedDict objects internally
        # This means sorted output should be equal to sorted expected output if the test passes
        expected.sort()
        self.results.sort()

        for index in six.moves.range(len(expected) - 1):
            if ((index >= len(expected)) or (index >= len(self.results))):
                break

            expected_obj = json.loads(expected[index], object_pairs_hook=collections.OrderedDict)
            actual_obj = json.loads(self.results[index], object_pairs_hook=collections.OrderedDict)
            if (expected_obj != actual_obj):
                print("Expected result could not be found: {0}".format(str(expected_obj)))
                success = False

        return success

class TestProg(unittest.TestCase):
    """Tests unit integration and invocation of the app from the command line
    """

    @staticmethod
    def data_path():
        return os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))

    @staticmethod
    def load_expected(tc_name):
        return_val = []

        expected_filename = os.path.join(TestProg.data_path(), "{0}_expected.jsonlines".format(tc_name))
        with open(expected_filename, "r") as f:
            for line in f:
                line = line.strip()
                return_val.append(line)

        return return_val

    def test_query_compile_error(self):
        runner = AppRunner()
        runner.run('#fro')
        self.assertEquals(runner.exit_code, 1)

    def test_query_run_error(self):
        runner = AppRunner()
        runner.run('#from "csv:{0}" #as ips #pivot-to "jsl:{1}" #as flows #where ips.ip = flows.src_ip or ips.ip = flows.dest_ip'.format(os.path.join(TestProg.data_path(), "file-that-does-not-exist"), os.path.join(TestProg.data_path(), "file-that-does-not-exist-2")))
        self.assertEquals(runner.exit_code, 1)

    def test_pivot_query(self):
        runner = AppRunner()
        runner.run('#from "csv:{0}" #as ips #pivot-to "jsl:{1}" #as flows #where ips.ip = flows.src_ip or ips.ip = flows.dest_ip'.format(os.path.join(TestProg.data_path(), "iplist.csv"), os.path.join(TestProg.data_path(), "eve-flow.jsonlines")))
        self.assertEquals(runner.exit_code, 0)

        expected = TestProg.load_expected("pivot_query")
        self.assertTrue(runner.check_results(expected))

    def test_no_el_config(self):
        runner = AppRunner()
        runner.run('#from "el:test_index" #as test1 #pivot-to "el:test_index_2" #as test2 #where test1.f = test2.f')
        self.assertEquals(runner.exit_code, 1)

    def test_el_bad_config(self):
        config = tempfile.mkstemp()
        try:
            with open(config[1], "w") as f:
                f.write('{"data_sources":{"elasticsearch":{}, "csv":{"first_row_names":true}}}')
            runner = AppRunner()
            runner.run('#from "el:test_index" #as test1 #pivot-to "el:test_index_2" #as test2 #where test1.f = test2.f', config_file = config[1])
            self.assertEquals(runner.exit_code, 1)
        finally:
            os.unlink(config[1])


