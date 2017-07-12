from __future__ import print_function
import logging
import string
import sys
import time

from jiggleq.query import JiggleQ
from jiggleq.relations import F

class TestResult(object):
    def __init__(self, data):
        self._data = data

    def to_dict(self):
        return self._data

class TestResults(object):
    def __init__(self, id_field_name, size):
        self._size = size
        self._cursor = 0
        self._id_field_name = id_field_name
        self._last_progress = 0.0
        self._progress = 0.0
        self._progress_displayed = False

    def __iter__(self):
        return self

    def next(self):
        if (self._cursor < self._size):
            next_record = {self._id_field_name:self._cursor,"name":"name{0}".format(self._cursor)}
            self._cursor += 1

            self._progress = round((float(self._cursor) / float(self._size)) * 100, 1)

            if (self._progress - self._last_progress >= 1.0):
                self._print_progress()
                self._last_progress = self._progress

            return TestResult(next_record).to_dict()
        else:
            self._progress = 100.0
            self._print_progress()
            print("\n")
            raise StopIteration

    def success(self):
        return True

    def _print_progress(self):
        self._progress_displayed = True

class TestResultHandler(object):

    def __init__(self):
        self.result_count = 0

    def __call__(self, result, handler_output):
        self.result_count += 1

    def success(self):
        return True

class MockDataSource(object):
    def __init__(self, id_field_name, size, step_name):
        self._step_name = step_name
        self._id_field_name = id_field_name
        self._size = size

    def batch(self):
        print("{0} ({1})".format(self._step_name, self._size))
        return TestResults(self._id_field_name, self._size)

def pivot_and_join(sizes):
    r = TestResultHandler()
    q1 = MockDataSource("id", sizes[0], "Step 1")
    q2 = MockDataSource("second_id", sizes[1], "Step 2")
    q3 = MockDataSource("third_id", sizes[2], "Step 3")

    s = JiggleQ(q1).pivot_to(q2, F("id") == F("second_id")).join_to(q3, F("second_id") == F("third_id"))
    s.result_handler(r)

    t_start = time.time()
    s.execute(stream=False)
    t_end = time.time()

    return round(t_end - t_start, 1)

def pivot_only(sizes):
    r = TestResultHandler()
    q1 = MockDataSource("id", sizes[0], "Step 1")
    q2 = MockDataSource("second_id", sizes[1], "Step 2")
    q3 = MockDataSource("third_id", sizes[2], "Step 3")

    s = JiggleQ(q1).pivot_to(q2, F("id") == F("second_id")).pivot_to(q3, F("second_id") == F("third_id"))
    s.result_handler(r)

    t_start = time.time()
    s.execute(stream=False)
    t_end = time.time()
    
    return round(t_end - t_start, 1)

def join_only(sizes):
    r = TestResultHandler()
    q1 = MockDataSource("id", sizes[0], "Step 1")
    q2 = MockDataSource("second_id", sizes[1], "Step 2")
    q3 = MockDataSource("third_id", sizes[2], "Step 3")

    s = JiggleQ(q1).join_to(q2, F("id") == F("second_id")).join_to(q3, F("second_id") == F("third_id"))
    s.result_handler(r)

    t_start = time.time()
    s.execute(stream=False)
    t_end = time.time()
    
    return round(t_end - t_start, 1)

def run_tc(name, logic, sizes):
    print("=== Test Case: {0} ===".format(name))

    test_results = []

    for num in xrange(3):
        test_results.append(logic(sizes))
        print("Run {0}: {1} second(s)".format(num + 1, test_results[-1]))

    total = 0
    for test_result in test_results:
        total += test_result

    average = round(float(total / len(test_results)), 2)
    print("Average: {0} second(s)".format(average))
    print("=== ===\n")



def run():
    run_tc("Pivot then join with exponential increase from seed", pivot_and_join, (1000, 10000, 1000000))
    run_tc("Pivot with exponential increase from seed", pivot_only, (1000, 10000, 1000000))
    run_tc("Join with exponential increase from seed", join_only, (1000, 10000, 1000000))

    return True

