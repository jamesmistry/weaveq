"""@package query_test
Tests for weaveq.query
"""

import unittest
import logging
import operator

from weaveq.query import IndexResultHandler
from weaveq.query import NestedField
from weaveq.query import WeaveQ
from weaveq.relations import F
from weaveq.relations import ConditionNode

class FirstCharProxy(object):
    """Applies proxy logic that represents values as their first character only.
    """
    def __init__(self, valid_names):
        self.count = 0
        self._valid_names = valid_names
        self.invalid_name = False

    def __call__(self, name, value):
        if (name not in self._valid_names):
           self.invalid_name = True

        self.count += 1
        return value[0]

class TestResult(object):
    def __init__(self, data):
        self._data = data

    def to_dict(self):
        return self._data

class TestResults(object):
    def __init__(self, data, success=True):
        self._simulated_success = success
        self._data = []
        for obj in data:
            self._data.append(TestResult(obj).to_dict())

    def __iter__(self):
        return self._data.__iter__()

    def success(self):
        return self._simulated_success

class TestResultHandler(object):

    def __init__(self, success=True):
        self._simulated_success = success
        self.results = []

    def __call__(self, result, handler_output):
        self.results.append(result)

    def success(self):
        return self._simulated_success

class TransparentFieldProxy(object):
    def __call__(self, name, value):
        return value

class MockDataSource(object):
    """Supplies pre-defined data to WeaveQ as if it were an Elasticsearch data source
    """

    def __str__(self):
        """Data forms string representation for use in assertions"""
        return "<data={0}>".format(id(self.data))

    def __init__(self, obj_array, success=True):
        """Constructor.
        """
        self._simulated_success = success
        self.data = obj_array
        self._cursor = 0

    def rewind(self):
        """Return to the beginning of the object array in servicing WeaveQ data requests"""
        self._cursor = 0

    def batch(self):
        """Services a WeaveQ data request, returning the next preloaded Python object"""
        return_val = None

        if (self._cursor < len(self.data)):
            return_val = TestResults(self.data[self._cursor], success=self._simulated_success)
            self._cursor += 1

        return return_val

class TestNestedField(unittest.TestCase):
    """Tests NestedField class
    """

    def test_top_level(self):
        """Existence and read test for top-level fields
        """

        subject = NestedField({"test_field" : "test_value"}, "test_field")
        self.assertTrue(subject.exists())
        self.assertEqual(subject.value(), "test_value")
        
    def test_exists_second_level(self):
        """Existence and read test for second-level fields
        """

        subject = NestedField({"test_field1" : { "test_field2" : "test_value" }}, "test_field1.test_field2")
        self.assertTrue(subject.exists())
        self.assertEqual(subject.value(), "test_value")

    def test_exists_third_level(self):
        """Existence and read test for third-level fields
        """

        subject = NestedField({"test_field1" : { "test_field2" : { "test_field3" : "test_value" } } }, "test_field1.test_field2.test_field3")
        self.assertTrue(subject.exists())
        self.assertEqual(subject.value(), "test_value")

    def test_not_exists(self):
        """Value non-existence
        """
        subject = NestedField({"test_field" : "test_value"}, "nonexistent_field")
        self.assertFalse(subject.exists())

        subject = NestedField({"test_field1" : { "test_field2" : "test_value" }}, "test_field2")
        self.assertFalse(subject.exists())

    def test_value_cache(self):
        """After the first read, target value comes from cache
        """
        data = {"test_field1" : { "test_field2" : "test_value" }}
        subject = NestedField(data, "test_field1.test_field2")
        self.assertEqual(subject.value(), "test_value")
        data["test_field1"]["test_field2"] = "test_new_value"
        self.assertEqual(subject.value(), "test_value")

    def test_value_cache_clear(self):
        """Target value cache is cleared correctly.
        """
        data = {"test_field1" : { "test_field2" : "test_value" }}
        subject = NestedField(data, "test_field1.test_field2")
        self.assertEqual(subject.value(), "test_value")
        data["test_field1"]["test_field2"] = "test_new_value"
        subject.clear_cache()
        self.assertEqual(subject.value(), "test_new_value")

class TestIndexResultHandler(unittest.TestCase):
        """Tests IndexResultHandler
        """

        def test_no_index_conds(self):
            """No index conditions provided
            """

            subject = IndexResultHandler([])

            result = []
            subject({"test_field" : "test_value"}, result)

            self.assertEqual(len(result), 0)
            
            self.assertFalse(subject.success()) # No condition group field dependencies were satisfied

        def test_single_cond_group_missing_field(self):
            """One field dependency of a condition in a condition group is missing in the target data
            """

            cond1 = ConditionNode(None, "test_lhs_field1", F.OP_EQ, "test_rhs_field1", TransparentFieldProxy(), TransparentFieldProxy())
            cond2 = ConditionNode(None, "test_lhs_field2", F.OP_NE, "test_rhs_field2", TransparentFieldProxy(), TransparentFieldProxy())

            subject = IndexResultHandler([[cond1, cond2]])

            result = []
            subject({cond1.left_field : "test_value"}, result)

            self.assertEqual(len(result), 1) # An index dictionary per condition group
            self.assertEqual(len(result[0][F.OP_EQ]), 0) # No EQ index entries because of the missing field
            self.assertEqual(len(result[0][F.OP_NE]), 0) # No NE index entries because of the missing field

            self.assertFalse(subject.success()) # No condition group field dependencies were satisfied

        def test_different_cond_group_missing_field(self):
            """2 condition groups: 1 has fully-satisfied field depencies, the other doesn't
            """

            cond1 = ConditionNode(None, "test_lhs_field1", F.OP_EQ, "test_rhs_field1", TransparentFieldProxy(), TransparentFieldProxy())
            cond2 = ConditionNode(None, "test_lhs_field2", F.OP_NE, "test_rhs_field2", TransparentFieldProxy(), TransparentFieldProxy())

            subject = IndexResultHandler([[cond1], [cond2]])

            result = []
            subject({cond1.left_field : "test_value"}, result)

            self.assertEqual(len(result), 2) # An index dictionary per condition group
            self.assertEqual(len(result[0][F.OP_EQ]), 1) # EQ index entry because its field dependencies are fully satisfied
            self.assertEqual(len(result[0][F.OP_NE]), 0) # No NE index entries because of the missing field

            self.assertTrue(subject.success()) # At least 1 condition group's field dependencies were satisfied

        def test_eq_cond_group_index(self):
            """1 EQ condition group with fully-satisfied field depencies that creates 1 index entry for an object
            """
    
            cond1 = ConditionNode(None, "test_lhs_field1", F.OP_EQ, "test_rhs_field1", TransparentFieldProxy(), TransparentFieldProxy())
    
            subject = IndexResultHandler([[cond1]])
        
            result = []
            data = {cond1.left_field : "test_value"}
            subject(data, result)
        
            self.assertEqual(len(result), 1) # An index dictionary per condition group
            self.assertEqual(len(result[0][F.OP_EQ]), 1) # EQ index entry because its field dependencies are fully satisfied
            self.assertEqual(len(result[0][F.OP_NE]), 0) # No NE index entries because there are no NE conditions

            self.assertEqual(len(result[0][F.OP_EQ][((0, "test_value"),)]), 1)
            self.assertEqual(result[0][F.OP_EQ][((0, "test_value"),)][0], data) # There's an index entry for condition 0 in condition group 0 with a value of "test_value"

            self.assertTrue(subject.success()) # At least 1 condition group's field dependencies were satisfied

        def test_eq_cond_group_multi_index_entries(self):
            """1 EQ condition group with fully-satisfied field depencies that creates 2 index entries for an object
            """
    
            cond1 = ConditionNode(None, "test_lhs_field1", F.OP_EQ, "test_rhs_field1", TransparentFieldProxy(), TransparentFieldProxy())
    
            subject = IndexResultHandler([[cond1]])
   
            result = []
            data1 = {cond1.left_field : "test_value", "name" : "data1"}
            data2 = {cond1.left_field : "test_value", "name" : "data2"}
            subject(data1, result)
            subject(data2, result)
       
            self.assertEqual(len(result), 1) # An index dictionary per condition group
            self.assertEqual(len(result[0][F.OP_EQ]), 1) # EQ index entry because its field dependencies are fully satisfied
            self.assertEqual(len(result[0][F.OP_NE]), 0) # No NE index entries because there are no NE conditions
            
            self.assertEqual(len(result[0][F.OP_EQ][((0, "test_value"),)]), 2) # Both data1 and data2 should be indexed on the target field
            self.assertEqual(result[0][F.OP_EQ][((0, "test_value"),)][0], data1)
            self.assertEqual(result[0][F.OP_EQ][((0, "test_value"),)][1], data2)

            self.assertTrue(subject.success()) # At least 1 condition group's field dependencies were satisfied

        def test_ne_cond_group_index(self):
            """1 NE condition group with fully-satisfied field depencies that creates 1 index entry for an object
            """

            cond1 = ConditionNode(None, "test_lhs_field1", F.OP_NE, "test_rhs_field1", TransparentFieldProxy(), TransparentFieldProxy())

            subject = IndexResultHandler([[cond1]])

            result = []
            data = {cond1.left_field : "test_value"}
            subject(data, result)
        
            self.assertEqual(len(result), 1) # An index dictionary per condition group
            self.assertEqual(len(result[0][F.OP_EQ]), 0) # No EQ index entry because there are no EQ conditions
            self.assertEqual(len(result[0][F.OP_NE]), 1) # NE index entry because its field dependencies are fully satisfied
            
            self.assertEqual(len(result[0][F.OP_NE][(0, "test_value")]), 1)
            self.assertEqual(result[0][F.OP_NE][(0, "test_value")][0], data) # There's an index entry for condition 0 in condition group 0 with a value of "test_value"

            self.assertTrue(subject.success()) # At least 1 condition group's field dependencies were satisfied

        def test_ne_cond_group_multi_index_entries(self):
            """1 NE condition group with fully-satisfied field depencies that creates 2 index entries for an object
            """
    
            cond1 = ConditionNode(None, "test_lhs_field1", F.OP_NE, "test_rhs_field1", TransparentFieldProxy(), TransparentFieldProxy())
    
            subject = IndexResultHandler([[cond1]])
    
            result = []
            data1 = {cond1.left_field : "test_value", "name" : "data1"}
            data2 = {cond1.left_field : "test_value", "name" : "data2"}
            subject(data1, result)
            subject(data2, result)

            self.assertEqual(len(result), 1) # An index dictionary per condition group
            self.assertEqual(len(result[0][F.OP_EQ]), 0) # No EQ index entry because there are no EQ conditions
            self.assertEqual(len(result[0][F.OP_NE]), 1) # NE index entry because its field dependencies are fully satisfied
            
            self.assertEqual(len(result[0][F.OP_NE][(0, "test_value")]), 2) # Both data1 and data2 should be indexed on the target field
            self.assertEqual(result[0][F.OP_NE][(0, "test_value")][0], data1)
            self.assertEqual(result[0][F.OP_NE][(0, "test_value")][1], data2)

            self.assertTrue(subject.success()) # At least 1 condition group's field dependencies were satisfied

        def test_eq_ne_conds_same_group(self):
            """Mixed EQ and NE conditions with fully-satisfied field depencies
            """

            cond1 = ConditionNode(None, "test_lhs_field1", F.OP_EQ, "test_rhs_field1", TransparentFieldProxy(), TransparentFieldProxy())
            cond2 = ConditionNode(None, "test_lhs_field2", F.OP_NE, "test_rhs_field2", TransparentFieldProxy(), TransparentFieldProxy())

            subject = IndexResultHandler([[cond1, cond2]])
   
            result = []
            data1 = {cond1.left_field : "test_value1", cond2.left_field : "test_value2", "name" : "data1"}
            data2 = {cond1.left_field : "test_value3", cond2.left_field : "test_value4", "name" : "data2"}
            subject(data1, result)
            subject(data2, result)

            self.assertEqual(len(result), 1) # An index dictionary per condition group
            self.assertEqual(len(result[0][F.OP_EQ]), 2) # EQ index entry for each object because its field dependencies are fully satisfied
            self.assertEqual(len(result[0][F.OP_NE]), 2) # NE index entry for each object because its field dependencies are fully satisfied

            self.assertEqual(len(result[0][F.OP_EQ][((0, "test_value1"),)]), 1) # data1 should be indexed on the target field
            self.assertEqual(len(result[0][F.OP_EQ][((0, "test_value3"),)]), 1) # data2 should be indexed on the target field
            self.assertEqual(result[0][F.OP_EQ][((0, "test_value1"),)][0], data1)
            self.assertEqual(result[0][F.OP_EQ][((0, "test_value3"),)][0], data2)

            self.assertEqual(len(result[0][F.OP_NE][(1, "test_value2")]), 1) # data1 should be indexed on the target field
            self.assertEqual(len(result[0][F.OP_NE][(1, "test_value4")]), 1) # data2 should be indexed on the target field
            self.assertEqual(result[0][F.OP_NE][(1, "test_value2")][0], data1)
            self.assertEqual(result[0][F.OP_NE][(1, "test_value4")][0], data2)

            self.assertTrue(subject.success()) # At least 1 condition group's field dependencies were satisfied

        def test_eq_ne_conds_diff_group(self):
            """Mixed EQ and NE conditions with fully-satisfied field depencies
            """

            cond1 = ConditionNode(None, "test_lhs_field1", F.OP_EQ, "test_rhs_field1", TransparentFieldProxy(), TransparentFieldProxy())
            cond2 = ConditionNode(None, "test_lhs_field2", F.OP_NE, "test_rhs_field2", TransparentFieldProxy(), TransparentFieldProxy())

            subject = IndexResultHandler([[cond1], [cond2]])
    
            result = []
            data1 = {cond1.left_field : "test_value1", cond2.left_field : "test_value2", "name" : "data1"}
            data2 = {cond1.left_field : "test_value3", cond2.left_field : "test_value4", "name" : "data2"}
            subject(data1, result)
            subject(data2, result)
        
            self.assertEqual(len(result), 2) # An index dictionary per condition group
            self.assertEqual(len(result[0][F.OP_EQ]), 2) # EQ index entry for each object because its field dependencies are fully satisfied
            self.assertEqual(len(result[1][F.OP_NE]), 2) # NE index entry for each object because its field dependencies are fully satisfied

            self.assertEqual(len(result[0][F.OP_EQ][((0, "test_value1"),)]), 1) # data1 should be indexed on the target field
            self.assertEqual(len(result[0][F.OP_EQ][((0, "test_value3"),)]), 1) # data2 should be indexed on the target field
            self.assertEqual(result[0][F.OP_EQ][((0, "test_value1"),)][0], data1)
            self.assertEqual(result[0][F.OP_EQ][((0, "test_value3"),)][0], data2)

            self.assertEqual(len(result[1][F.OP_NE][(0, "test_value2")]), 1) # data1 should be indexed on the target field
            self.assertEqual(len(result[1][F.OP_NE][(0, "test_value4")]), 1) # data2 should be indexed on the target field
            self.assertEqual(result[1][F.OP_NE][(0, "test_value2")][0], data1)
            self.assertEqual(result[1][F.OP_NE][(0, "test_value4")][0], data2)

            self.assertTrue(subject.success()) # At least 1 condition group's field dependencies were satisfied


class TestWeaveQ(unittest.TestCase):
    """Tests WeaveQ class
    """

    def test_seed(self):
        """Ensure the seed query is executed"""
        r = TestResultHandler()
        q1 = MockDataSource([[{"name":"test1","number":0},{"name":"test2","number":1}]])
        s = WeaveQ(q1)
        s.result_handler(r)
        s.execute(stream=False)

        self.assertEqual(r.results, q1.data[0])

    def test_pivot_one_to_one(self):
        """Pivot test: equality, single relationship, one-to-one results"""
        r = TestResultHandler()
        q1 = MockDataSource([[{"id":1,"name":"record_a"},{"id":2,"name":"record_b"},{"id":3,"name":"record_c"},{"id":4,"name":"record_b"}]])
        q2 = MockDataSource([[{"name_id":1,"count":10},{"name_id":6,"count":11},{"name_id":5,"count":12},{"name_id":4,"count":13}]])
        s = WeaveQ(q1).pivot_to(q2, F("id") == F("name_id"))
        s.result_handler(r)
        s.execute(stream=False)

        self.assertEqual(r.results, [{"name_id":1,"count":10},{"name_id":4,"count":13}])

    def test_pivot_one_to_many(self):
        """Pivot test: equality, single relationship, one-to-many results"""
        r = TestResultHandler()
        q1 = MockDataSource([[{"id":1,"name":"record_a"},{"id":2,"name":"record_b"},{"id":3,"name":"record_c"},{"id":4,"name":"record_b"}]])
        q2 = MockDataSource([[{"name_id":1,"count":10},{"name_id":1,"count":11},{"name_id":5,"count":12},{"name_id":4,"count":13},{"name_id":4,"count":14}]])
        s = WeaveQ(q1).pivot_to(q2, F("id") == F("name_id"))
        s.result_handler(r)
        s.execute(stream=False)

        self.assertEqual(r.results, [{"name_id":1,"count":10},{"name_id":1,"count":11},{"name_id":4,"count":13},{"name_id":4,"count":14}])

    def test_pivot_multi_cond(self):
        """Pivot test: equality, multiple relationships"""
        r = TestResultHandler()
        q1 = MockDataSource([[{"id":1,"name":"w","value":"record_a"},{"id":2,"name":"x","value":"record_b"},{"id":3,"name":"y","value":"record_c"},{"id":4,"name":"z","value":"record_d"}]])
        q2 = MockDataSource([[{"name_id":1,"letter":"w","count":10},{"name_id":1,"letter":"y","count":11},{"name_id":5,"letter":"z","count":12},{"name_id":3,"letter":"y","count":13},{"name_id":4,"letter":"a","count":14}]])
        s = WeaveQ(q1).pivot_to(q2, (F("id") == F("name_id")) & (F("name") == F("letter")))
        s.result_handler(r)
        s.execute(stream=False)

        self.assertEqual(r.results, [{"name_id":1,"letter":"w","count":10},{"name_id":3,"letter":"y","count":13}])

    def test_pivot_multi_groups(self):
        """Pivot test: equality, multiple relationship groups"""
        r = TestResultHandler()
        q1 = MockDataSource([[{"id":1,"name":"w","value":"record_a","target_count":11},{"id":2,"name":"x","value":"record_b","target_count":12},{"id":3,"name":"y","value":"record_c","target_count":14},{"id":4,"name":"z","value":"record_d","target_count":15}]])
        q2 = MockDataSource([[{"name_id":1,"letter":"w","count":10},{"name_id":1,"letter":"y","count":11},{"name_id":5,"letter":"z","count":16},{"name_id":3,"letter":"y","count":13},{"name_id":4,"letter":"a","count":15}]])
        s = WeaveQ(q1).pivot_to(q2, ((F("id") == F("name_id")) & (F("name") == F("letter"))) | (F("target_count") == F("count")))
        s.result_handler(r)
        s.execute(stream=False)

        self.assertEqual(r.results, [{"name_id":1,"letter":"w","count":10},{"name_id":1,"letter":"y","count":11},{"name_id":3,"letter":"y","count":13},{"name_id":4,"letter":"a","count":15}])

    def test_pivot_ne(self):
        """Pivot test: inequality, single relationship"""
        r = TestResultHandler()
        q1 = MockDataSource([[{"id":1,"name":"w","value":"record_a","target_count":11},{"id":2,"name":"x","value":"record_b","target_count":12},{"id":3,"name":"y","value":"record_c","target_count":14},{"id":4,"name":"z","value":"record_d","target_count":15}]])
        q2 = MockDataSource([[{"name_id":1,"letter":"w","count":10},{"name_id":1,"letter":"y","count":11},{"name_id":5,"letter":"z","count":16},{"name_id":3,"letter":"y","count":13},{"name_id":4,"letter":"a","count":15}]])
        s = WeaveQ(q1).pivot_to(q2, F("id") != F("name_id"))
        s.result_handler(r)
        s.execute(stream=False)

        self.assertEqual(r.results, [{"name_id":5,"letter":"z","count":16}])

    def test_pivot_ne_multi_cond(self):
        """Pivot test: inequality, multiple relationships"""
        r = TestResultHandler()
        q1 = MockDataSource([[{"id":1,"name":"w","value":"record_a","target_count":11},{"id":2,"name":"x","value":"record_b","target_count":12},{"id":3,"name":"y","value":"record_c","target_count":14},{"id":4,"name":"z","value":"record_d","target_count":15}]])
        q2 = MockDataSource([[{"name_id":1,"letter":"w","count":10},{"name_id":6,"letter":"y","count":10},{"name_id":5,"letter":"z","count":12},{"name_id":3,"letter":"y","count":13},{"name_id":4,"letter":"a","count":15}]])
        s = WeaveQ(q1).pivot_to(q2, (F("id") != F("name_id")) & (F("target_count") != F("count")))
        s.result_handler(r)
        s.execute(stream=False)

        self.assertEqual(r.results, [{"name_id":6,"letter":"y","count":10}])

    def test_pivot_ne_multi_groups(self):
        """Pivot test: inequality, multiple relationship groups"""
        r = TestResultHandler()
        q1 = MockDataSource([[{"id":1,"name":"w","value":"record_a","target_count":11},{"id":2,"name":"x","value":"record_b","target_count":12},{"id":3,"name":"y","value":"record_c","target_count":14},{"id":4,"name":"z","value":"record_d","target_count":15}]])
        q2 = MockDataSource([[{"name_id":1,"letter":"w","count":10},{"name_id":6,"letter":"y","count":10},{"name_id":5,"letter":"z","count":12},{"name_id":3,"letter":"y","count":13},{"name_id":4,"letter":"a","count":15}]])
        s = WeaveQ(q1).pivot_to(q2, (F("id") != F("name_id")) | (F("target_count") != F("count")))
        s.result_handler(r)
        s.execute(stream=False)

        self.assertEqual(r.results, [{"name_id":1,"letter":"w","count":10},{"name_id":6,"letter":"y","count":10},{"name_id":5,"letter":"z","count":12},{"name_id":3,"letter":"y","count":13}])

    def test_pivot_ne_and_eq(self):
        """Pivot test: inequality and equality relationships"""
        r = TestResultHandler()
        q1 = MockDataSource([[{"id":1,"name":"w","value":"record_a","target_count":11},{"id":2,"name":"x","value":"record_b","target_count":12},{"id":6,"name":"y","value":"record_c","target_count":10},{"id":4,"name":"z","value":"record_d","target_count":10}]])
        q2 = MockDataSource([[{"name_id":1,"letter":"w","count":10},{"name_id":6,"letter":"y","count":10},{"name_id":5,"letter":"z","count":12},{"name_id":3,"letter":"y","count":13},{"name_id":4,"letter":"a","count":15}]])
        s = WeaveQ(q1).pivot_to(q2, (F("id") != F("name_id")) & (F("target_count") == F("count")))
        s.result_handler(r)
        s.execute(stream=False)

        self.assertEqual(r.results, [{"name_id":5,"letter":"z","count":12}])

    def test_join_ne_and_eq(self):
        """Join test: inequality and equality relationships"""
        r = TestResultHandler()
        q1 = MockDataSource([[{"id":1,"name":"a"},{"id":2,"name":"a"},{"id":1,"name":"c"},{"id":3,"name":"f"}]])
        q2 = MockDataSource([[{"name_id":2,"type":"a"}]])
        s = WeaveQ(q1).join_to(q2, (F("id") != F("name_id")) & (F("name") == F("type")))
        s.result_handler(r)
        s.execute(stream=False)

        self.assertEqual(r.results, [{"name_id":2,"type":"a","joined_data":{"id":1,"name":"a"}}]) 

    def test_join_ne(self):
        """Join test: inequality relationship"""
        r = TestResultHandler()
        q1 = MockDataSource([[{"id":1,"name":"a"},{"id":1,"name":"a"},{"id":1,"name":"c"},{"id":3,"name":"f"}]])
        q2 = MockDataSource([[{"name_id":1,"type":"a"}]])
        s = WeaveQ(q1).join_to(q2, (F("id") != F("name_id")))
        s.result_handler(r)
        s.execute(stream=False)

        self.assertEqual(r.results, [{"name_id":1,"type":"a","joined_data":{"id":3,"name":"f"}}])

    def test_join_eq(self):
        """Join test: equality relationship"""
        r = TestResultHandler()
        q1 = MockDataSource([[{"id":1,"name":"a"},{"id":1,"name":"a"},{"id":1,"name":"c"},{"id":3,"name":"f"}]])
        q2 = MockDataSource([[{"name_id":3,"type":"a"}]])
        s = WeaveQ(q1).join_to(q2, (F("id") == F("name_id")))
        s.result_handler(r)
        s.execute(stream=False)

        self.assertEqual(r.results, [{"name_id":3,"type":"a","joined_data":{"id":3,"name":"f"}}])

    def test_join_custom_field_ne_and_eq(self):
        """Join test: inequality relationship, custom field"""
        r = TestResultHandler()
        q1 = MockDataSource([[{"id":1,"name":"a"},{"id":1,"name":"a"},{"id":1,"name":"a"},{"id":3,"name":"a"}]])
        q2 = MockDataSource([[{"name_id":1,"type":"a"}]])

        s = WeaveQ(q1).join_to(q2, (F("id") != F("name_id")) & (F("name") == F("type")), field="custom_field_name")
        s.result_handler(r)
        s.execute(stream=False)
        self.assertEqual(r.results, [{"name_id":1,"type":"a","custom_field_name":{"id":3,"name":"a"}}])

    def test_join_custom_field_ne(self):
        """Join test: inequality relationship, custom field"""
        r = TestResultHandler()
        q1 = MockDataSource([[{"id":1,"name":"a"},{"id":1,"name":"a"},{"id":1,"name":"c"},{"id":3,"name":"f"}]])
        q2 = MockDataSource([[{"name_id":1,"type":"a"}]])

        s = WeaveQ(q1).join_to(q2, (F("id") != F("name_id")), field="custom_field_name")
        s.result_handler(r)
        s.execute(stream=False)
        self.assertEqual(r.results, [{"name_id":1,"type":"a","custom_field_name":{"id":3,"name":"f"}}])

    def test_join_custom_field_eq(self):
        """Join test: equality relationship, custom field"""
        r = TestResultHandler()
        q1 = MockDataSource([[{"id":1,"name":"a"},{"id":1,"name":"a"},{"id":1,"name":"c"},{"id":3,"name":"f"}]])
        q2 = MockDataSource([[{"name_id":3,"type":"a"}]])

        s = WeaveQ(q1).join_to(q2, (F("id") == F("name_id")), field="custom_field_name")
        s.result_handler(r)
        s.execute(stream=False)
        self.assertEqual(r.results, [{"name_id":3,"type":"a","custom_field_name":{"id":3,"name":"f"}}])

    def test_join_custom_field_ne_and_eq(self):
        """Join test: inequality and equality relationship, custom field"""
        r = TestResultHandler()
        q1 = MockDataSource([[{"id":1,"name":"a"},{"id":1,"name":"a"},{"id":1,"name":"a"},{"id":3,"name":"a"}]])
        q2 = MockDataSource([[{"name_id":1,"type":"a"}]])

        s = WeaveQ(q1).join_to(q2, (F("id") != F("name_id")) & (F("name") == F("type")), field="custom_field_name")
        s.result_handler(r)
        s.execute(stream=False)
        self.assertEqual(r.results, [{"name_id":1,"type":"a","custom_field_name":{"id":3,"name":"a"}}])

    def test_join_array_ne(self):
        """Join test: inequality relationship, array"""
        r = TestResultHandler()
        q1 = MockDataSource([[{"id":1,"name":"a"},{"id":2,"name":"a"},{"id":3,"name":"c"},{"id":4,"name":"f"}]])
        q2 = MockDataSource([[{"name_id":1,"type":"a"}]])

        s = WeaveQ(q1).join_to(q2, (F("id") != F("name_id")), field="custom_field_name", array=True)
        s.result_handler(r)
        s.execute(stream=False)
        r.results[0]["custom_field_name"] = sorted(r.results[0]["custom_field_name"], key=operator.itemgetter("id"))
        self.assertEqual(r.results, [{"name_id":1,"type":"a","custom_field_name":[{"id":2,"name":"a"},{"id":3,"name":"c"},{"id":4,"name":"f"}]}])

    def test_join_array_eq(self):
        """Join test: equality relationship, array"""
        r = TestResultHandler()
        q1 = MockDataSource([[{"id":1,"name":"a"},{"id":1,"name":"b"},{"id":1,"name":"c"},{"id":3,"name":"f"}]])
        q2 = MockDataSource([[{"name_id":1,"type":"a"}]])

        s = WeaveQ(q1).join_to(q2, (F("id") == F("name_id")), field="custom_field_name", array=True)
        s.result_handler(r)
        s.execute(stream=False)
        r.results[0]["custom_field_name"] = sorted(r.results[0]["custom_field_name"], key=operator.itemgetter("name"))
        self.assertEqual(r.results, [{"name_id":1,"type":"a","custom_field_name":[{"id":1,"name":"a"},{"id":1,"name":"b"},{"id":1,"name":"c"}]}])

    def test_join_array_ne_and_eq(self):
        """Join test: inequality and equality relationship, array"""
        r = TestResultHandler()
        q1 = MockDataSource([[{"id":1,"name":"a"},{"id":2,"name":"a"},{"id":3,"name":"a"},{"id":4,"name":"b"}]])
        q2 = MockDataSource([[{"name_id":1,"type":"a"}]])

        s = WeaveQ(q1).join_to(q2, (F("id") != F("name_id")) & (F("name") == F("type")), field="custom_field_name", array=True)
        s.result_handler(r)
        s.execute(stream=False)
        r.results[0]["custom_field_name"] = sorted(r.results[0]["custom_field_name"], key=operator.itemgetter("id"))
        self.assertEqual(r.results, [{"name_id":1,"type":"a","custom_field_name":[{"id":2,"name":"a"},{"id":3,"name":"a"}]}])

    def test_join_array_include_empties(self):
        """Join test: exclude empty joins, array"""
        r = TestResultHandler()
        q1 = MockDataSource([[{"id":1,"name":"a"},{"id":1,"name":"b"},{"id":1,"name":"c"},{"id":3,"name":"f"}]])
        q2 = MockDataSource([[{"name_id":1,"type":"a"},{"name_id":99,"type":"a"}]])

        s = WeaveQ(q1).join_to(q2, (F("id") == F("name_id")), field="custom_field_name", array=True, exclude_empty_joins=False)
        s.result_handler(r)
        s.execute(stream=False)
        r.results[0]["custom_field_name"] = sorted(r.results[0]["custom_field_name"], key=operator.itemgetter("name"))
        self.assertEqual(r.results, [{"name_id":1,"type":"a","custom_field_name":[{"id":1,"name":"a"},{"id":1,"name":"b"},{"id":1,"name":"c"}]},{"name_id":99,"type":"a"}])

    def test_join_array_exclude_empties(self):
        """Join test: exclude empty joins, array"""
        r = TestResultHandler()
        q1 = MockDataSource([[{"id":1,"name":"a"},{"id":1,"name":"b"},{"id":1,"name":"c"},{"id":3,"name":"f"}]])
        q2 = MockDataSource([[{"name_id":1,"type":"a"},{"name_id":99,"type":"a"}]])

        s = WeaveQ(q1).join_to(q2, (F("id") == F("name_id")), field="custom_field_name", array=True, exclude_empty_joins=True)
        s.result_handler(r)
        s.execute(stream=False)
        r.results[0]["custom_field_name"] = sorted(r.results[0]["custom_field_name"], key=operator.itemgetter("name"))
        self.assertEqual(r.results, [{"name_id":1,"type":"a","custom_field_name":[{"id":1,"name":"a"},{"id":1,"name":"b"},{"id":1,"name":"c"}]}])

    def test_join_no_array_include_empties(self):
        """Join test: exclude empty joins, array"""
        r = TestResultHandler()
        q1 = MockDataSource([[{"id":1,"name":"a"},{"id":2,"name":"b"},{"id":3,"name":"c"},{"id":4,"name":"f"}]])
        q2 = MockDataSource([[{"name_id":1,"type":"a"},{"name_id":99,"type":"a"}]])

        s = WeaveQ(q1).join_to(q2, (F("id") == F("name_id")), field="custom_field_name", array=False, exclude_empty_joins=False)
        s.result_handler(r)
        s.execute(stream=False)
        self.assertEqual(r.results, [{"name_id":1,"type":"a","custom_field_name":{"id":1,"name":"a"}},{"name_id":99,"type":"a"}])

    def test_join_no_array_exclude_empties(self):
        """Join test: exclude empty joins, array"""
        r = TestResultHandler()
        q1 = MockDataSource([[{"id":1,"name":"a"},{"id":2,"name":"b"},{"id":3,"name":"c"},{"id":4,"name":"f"}]])
        q2 = MockDataSource([[{"name_id":1,"type":"a"},{"name_id":99,"type":"a"}]])

        s = WeaveQ(q1).join_to(q2, (F("id") == F("name_id")), field="custom_field_name", array=False, exclude_empty_joins=True)
        s.result_handler(r)
        s.execute(stream=False)
        self.assertEqual(r.results, [{"name_id":1,"type":"a","custom_field_name":{"id":1,"name":"a"}}])

    def test_join_non_first_or_expression_no_exclude_empties(self):
        """Join test: or'ed expressions are not short-cutted when including empty matches"""
        r = TestResultHandler()
        q1 = MockDataSource([[{"id":1,"name":"a"},{"id":2,"name":"b"},{"id":3,"name":"c"},{"id":4,"name":"f"}]])
        q2 = MockDataSource([[{"name_id":5,"type":"a"},{"name_id":6,"type":"b"},{"name_id":99,"type":"z"}]])

        s = WeaveQ(q1).join_to(q2, ((F("id") == F("name_id")) | (F("name") == F("type"))), field="custom_field_name", array=False, exclude_empty_joins=False)
        s.result_handler(r)
        s.execute(stream=False)
        self.assertEqual(r.results, [{"name_id":5,"type":"a","custom_field_name":{"id":1,"name":"a"}},{"name_id":6,"type":"b","custom_field_name":{"id":2,"name":"b"}},{"name_id":99,"type":"z"}])

    def test_join_non_first_or_expression_exclude_empties(self):
        """Join test: or'ed expressions are not short-cutted when not excluding empty matches"""
        r = TestResultHandler()
        q1 = MockDataSource([[{"id":1,"name":"a"},{"id":2,"name":"b"},{"id":3,"name":"c"},{"id":4,"name":"f"}]])
        q2 = MockDataSource([[{"name_id":5,"type":"a"},{"name_id":6,"type":"b"},{"name_id":99,"type":"z"}]])

        s = WeaveQ(q1).join_to(q2, ((F("id") == F("name_id")) | (F("name") == F("type"))), field="custom_field_name", array=False, exclude_empty_joins=True)
        s.result_handler(r)
        s.execute(stream=False)
        self.assertEqual(r.results, [{"name_id":5,"type":"a","custom_field_name":{"id":1,"name":"a"}},{"name_id":6,"type":"b","custom_field_name":{"id":2,"name":"b"}}])

    def test_join_no_array_multi_results(self):
        """Join test: exclude empty joins, array"""
        r = TestResultHandler()
        q1 = MockDataSource([[{"id":1,"name":"a"},{"id":1,"name":"b"},{"id":3,"name":"c"},{"id":4,"name":"f"}]])
        q2 = MockDataSource([[{"name_id":1,"type":"a"}]])

        s = WeaveQ(q1).join_to(q2, (F("id") == F("name_id")), field="custom_field_name", array=False)
        s.result_handler(r)
        s.execute(stream=False)
        self.assertEqual(r.results, [{"name_id":1,"type":"a","custom_field_name":{"id":1,"name":"a"}}])

    def test_proxy(self):
        """Proxy test"""
        p = FirstCharProxy({"type_l":None,"type_r":None})
        r = TestResultHandler()
        q1 = MockDataSource([[{"name":"a","type_l":"xca"},{"name":"b","type_l":"klm"},{"name":"c","type_l":"xyz"},{"name":"d","type_l":"ghi"}]])
        q2 = MockDataSource([[{"name_id":1,"type_r":"fxc"},{"name_id":1,"type_r":"xre"},{"name_id":1,"type_r":"xkk"}]])
        
        s = WeaveQ(q1).pivot_to(q2, (F("type_l", proxy=p) == F("type_r", proxy=p)))
        s.result_handler(r)
        s.execute(stream=False)

        self.assertFalse(p.invalid_name)
        self.assertEqual(p.count, 7)
        self.assertEqual(r.results, [{"name_id":1,"type_r":"xre"},{"name_id":1,"type_r":"xkk"}])

    def test_failed_result_handler(self):
        """Failed result handler test"""
        r = TestResultHandler(False)
        q1 = MockDataSource([[{"id":1,"name":"record_a"},{"id":2,"name":"record_b"},{"id":3,"name":"record_c"},{"id":4,"name":"record_b"}]])
        q2 = MockDataSource([[{"name_id":1,"count":10},{"name_id":6,"count":11},{"name_id":5,"count":12},{"name_id":4,"count":13}]])
        s = WeaveQ(q1).pivot_to(q2, F("id") == F("name_id"))
        s.result_handler(r)
        self.assertFalse(s.execute(stream=False))

    def test_failed_response(self):
        """Failed response test"""
        r = TestResultHandler()
        q1 = MockDataSource([[{"id":1,"name":"record_a"},{"id":2,"name":"record_b"},{"id":3,"name":"record_c"},{"id":4,"name":"record_b"}]], success=False)
        q2 = MockDataSource([[{"name_id":1,"count":10},{"name_id":6,"count":11},{"name_id":5,"count":12},{"name_id":4,"count":13}]])
        s = WeaveQ(q1).pivot_to(q2, F("id") == F("name_id"))
        s.result_handler(r)
        self.assertFalse(s.execute(stream=False))

    def test_missing_field(self):
        """Missing field"""
        r = TestResultHandler()
        q1 = MockDataSource([[{"id":1,"name":"record_a"},{"id":2,"name":"record_b"},{"id":3,"name":"record_c"},{"id":4,"name":"record_b"}]])
        q2 = MockDataSource([[{"data":"record_a"},{"data":"record_b"},{"name_id":1,"data":"record_a"},{"id":3,"dat":"record_c"}]])
        s = WeaveQ(q1).pivot_to(q2, (F("id") == F("name_id")) & (F("name") == F("data")))
        s.result_handler(r)
        s.execute(stream=False)

        self.assertEqual(r.results, [{"name_id":1,"data":"record_a"}])

    def test_multistep_pivot_pivot(self):
        r = TestResultHandler()
        q1 = MockDataSource([[{"id":1,"name":"record_a"},{"id":2,"name":"record_b"},{"id":2,"name":"record_c"},{"id":2,"name":"record_b"}]])
        q2 = MockDataSource([[{"name_id":2,"count":10,"data":"record_b"},{"name_id":2,"count":11,"data":"record_b"},{"name_id":3,"count":12,"data":"record_e"}]])
        q3 = MockDataSource([[{"id":20,"record":"record_e"},{"id":21,"record":"record_a"},{"id":30,"record":"record_b"},{"id":31,"record":"record_b"},]])
        s = WeaveQ(q1).pivot_to(q2, (F("id") == F("name_id"))).pivot_to(q3, F("data") == F("record"))
        s.result_handler(r)
        s.execute(stream=False)

        self.assertEqual(r.results, [{"id":30,"record":"record_b"},{"id":31,"record":"record_b"}])

    def test_multistep_join_join(self):
        r = TestResultHandler()
        q1 = MockDataSource([[{"id":1,"name":"record_a"},{"id":2,"name":"record_b"},{"id":3,"name":"record_c"},{"id":4,"name":"record_b"}]])
        q2 = MockDataSource([[{"name_id":2,"count":10,"data":"record_a"},{"name_id":6,"count":11,"data":"record_c"},{"name_id":5,"count":12,"data":"record_b"}]])
        q3 = MockDataSource([[{"id":20,"record":"record_c"},{"id":21,"record":"record_a"},{"id":30,"record":"record_b"},{"id":31,"record":"record_b"},]])
        s = WeaveQ(q1).join_to(q2, (F("id") == F("name_id")), field="step1", exclude_empty_joins=True).join_to(q3, F("data") == F("record"), field="step2", exclude_empty_joins=True)
        s.result_handler(r)
        s.execute(stream=False)

        self.assertEqual(r.results, [{"id":21,"record":"record_a","step2":{"name_id":2,"count":10,"data":"record_a","step1":{"id":2,"name":"record_b"}}}])

    def test_multistep_pivot_join(self):
        r = TestResultHandler()
        q1 = MockDataSource([[{"id":1,"name":"record_a"},{"id":2,"name":"record_b"},{"id":2,"name":"record_c"},{"id":2,"name":"record_b"}]])
        q2 = MockDataSource([[{"name_id":2,"count":10,"data":"record_b"},{"name_id":2,"count":11,"data":"record_d"},{"name_id":3,"count":12,"data":"record_e"}]])
        q3 = MockDataSource([[{"id":20,"record":"record_e"},{"id":21,"record":"record_a"},{"id":30,"record":"record_b"},{"id":31,"record":"record_b"},]])
        s = WeaveQ(q1).pivot_to(q2, (F("id") == F("name_id"))).join_to(q3, F("data") == F("record"), exclude_empty_joins=True)
        s.result_handler(r)
        s.execute(stream=False)

        self.assertEqual(r.results, [{"id":30,"record":"record_b","joined_data":{"name_id":2,"count":10,"data":"record_b"}},{"id":31,"record":"record_b","joined_data":{"name_id":2,"count":10,"data":"record_b"}}])

    def test_multistep_join_pivot(self):
        r = TestResultHandler()
        q1 = MockDataSource([[{"id":1,"name":"record_a"},{"id":2,"name":"record_b"},{"id":3,"name":"record_c"},{"id":4,"name":"record_b"}]])
        q2 = MockDataSource([[{"name_id":2,"count":10,"data":"record_a"},{"name_id":6,"count":11,"data":"record_c"},{"name_id":5,"count":12,"data":"record_b"}]])
        q3 = MockDataSource([[{"id":2,"record":"record_c"},{"id":21,"record":"record_a"},{"id":2,"record":"record_b"},{"id":31,"record":"record_b"},]])
        s = WeaveQ(q1).join_to(q2, (F("id") == F("name_id")), field="step1", exclude_empty_joins=True).pivot_to(q3, F("step1.id") == F("id"))
        s.result_handler(r)
        s.execute(stream=False)

        r.results = sorted(r.results, key=operator.itemgetter("record"))
        self.assertEqual(r.results, [{"id":2,"record":"record_b"},{"id":2,"record":"record_c"}])

    def test_query_as_str(self):
        q1 = MockDataSource([[{'id':1,'name':'record_a'},{'id':2,'name':'record_b'},{'id':3,'name':'record_c'},{'id':4,'name':'record_b'}]])
        q2 = MockDataSource([[{'name_id':2,'count':10,'data':'record_a'},{'name_id':6,'count':11,'data':'record_c'},{'name_id':5,'count':12,'data':'record_b'}]])
        q3 = MockDataSource([[{'id':2,'record':'record_c'},{'id':21,'record':'record_a'},{'id':2,'record':'record_b'},{'id':31,'record':'record_b'}]])
        s = WeaveQ(q1).join_to(q2, (F("id") == F("name_id")), field="step1", exclude_empty_joins=True).pivot_to(q3, F("step1.id") == F("id"))

        self.assertEqual(str(s), "<pos=0, op=SEED, q={0}>,<pos=1, op=JOIN, q={1}, rels=[[id == name_id]], exclude_empty=True, field_name=step1, array=False>,<pos=2, op=PIVOT, q={2}, rels=[[step1.id == id]]>".format(str(q1), str(q2), str(q3))) 

