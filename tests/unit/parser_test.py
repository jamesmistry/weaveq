"""@package parser_test
Tests for jiggleq.parser
"""

import unittest

from jiggleq.parser import TextQuery, DataSourceBuilder
from jiggleq.jqexception import TextQueryCompileError

class TestDataSource(object):
    """A data source mock that always succeeds
    """

    def __init__(self, source_uri, filter_string):
        self._uri = source_uri
        self._filter = filter_string

    def __str__(self):
        """String representation for use in assertions"""
        return "<uri={0}, filter={1}>".format(self._uri, str(self._filter))

class TestDataSourceBuilder(DataSourceBuilder):
    def __init__(self, fail = False):
        self._fail = fail

    def __call__(self, source_uri, filter_string):
        if (self._fail):
            return None        
        else:
            data_source = TestDataSource(source_uri, filter_string)
            return data_source

class TestTextQuery(unittest.TestCase):
    """Tests TextQuery class
    """

    def test_no_process_clause(self):
        """Text query with no process clause
        """
        data_source_builder = TestDataSourceBuilder()
        subject = TextQuery(data_source_builder)

        with self.assertRaises(TextQueryCompileError):
            subject.compile_query('#from "source1" #as a1 #filter |filter1|')

    def test_join(self):
        """Join clause, no options
        """
        data_source_builder = TestDataSourceBuilder()
        subject = TextQuery(data_source_builder)

        result = subject.compile_query('#from "source1" #as a1 #filter |filter1| #join-to "source2" #as a2 #where a1.field1 = a2.field2')

        self.assertEquals(str(result), "<pos=0, op=SEED, q={0}>,<pos=1, op=JOIN, q={1}, rels=[[field1 == field2]], exclude_empty=False, field_name=None, array=False>".format("<uri=source1, filter=filter1>", "<uri=source2, filter=None>"))

    def test_multi_step_process(self):
        """Join clause, no options
        """
        data_source_builder = TestDataSourceBuilder()
        subject = TextQuery(data_source_builder)

        result = subject.compile_query('#from "source1" #as a1 #filter |filter1| #join-to "source2" #as a2 #where a1.field1 = a2.field2 #pivot-to "source3" #as a3 #where a2.field1 = a3.field2 #pivot-to "source4" #as a4 #where a3.field2 = a4.field1')

        self.assertEquals(str(result), "<pos=0, op=SEED, q={0}>,<pos=1, op=JOIN, q={1}, rels=[[field1 == field2]], exclude_empty=False, field_name=None, array=False>,<pos=2, op=PIVOT, q={2}, rels=[[field1 == field2]]>,<pos=3, op=PIVOT, q={3}, rels=[[field2 == field1]]>".format("<uri=source1, filter=filter1>", "<uri=source2, filter=None>", "<uri=source3, filter=None>", "<uri=source4, filter=None>"))

    def test_join_filter(self):
        """Join clause with filter
        """
        data_source_builder = TestDataSourceBuilder()
        subject = TextQuery(data_source_builder)

        result = subject.compile_query('#from "source1" #as a1 #filter |filter1| #join-to "source2" #as a2 #filter |filter2| #where a1.field1 = a2.field2')

        self.assertEquals(str(result), "<pos=0, op=SEED, q={0}>,<pos=1, op=JOIN, q={1}, rels=[[field1 == field2]], exclude_empty=False, field_name=None, array=False>".format("<uri=source1, filter=filter1>", "<uri=source2, filter=filter2>"))

    def test_join_exclude_empty(self):
        """Join clause, exclude empty option
        """
        data_source_builder = TestDataSourceBuilder()
        subject = TextQuery(data_source_builder)

        result = subject.compile_query('#from "source1" #as a1 #filter |filter1| #join-to "source2" #as a2 #where a1.field1 = a2.field2 #exclude-empty')

        self.assertEquals(str(result), "<pos=0, op=SEED, q={0}>,<pos=1, op=JOIN, q={1}, rels=[[field1 == field2]], exclude_empty=True, field_name=None, array=False>".format("<uri=source1, filter=filter1>", "<uri=source2, filter=None>"))

    def test_join_array(self):
        """Join clause, array option
        """
        data_source_builder = TestDataSourceBuilder()
        subject = TextQuery(data_source_builder)

        result = subject.compile_query('#from "source1" #as a1 #filter |filter1| #join-to "source2" #as a2 #where a1.field1 = a2.field2 #array')

        self.assertEquals(str(result), "<pos=0, op=SEED, q={0}>,<pos=1, op=JOIN, q={1}, rels=[[field1 == field2]], exclude_empty=False, field_name=None, array=True>".format("<uri=source1, filter=filter1>", "<uri=source2, filter=None>"))
    
    def test_join_field_name(self):
        """Join clause, field name option
        """
        data_source_builder = TestDataSourceBuilder()
        subject = TextQuery(data_source_builder)

        result = subject.compile_query('#from "source1" #as a1 #filter |filter1| #join-to "source2" #as a2 #where a1.field1 = a2.field2 #field-name test_name')

        self.assertEquals(str(result), "<pos=0, op=SEED, q={0}>,<pos=1, op=JOIN, q={1}, rels=[[field1 == field2]], exclude_empty=False, field_name=test_name, array=False>".format("<uri=source1, filter=filter1>", "<uri=source2, filter=None>"))

    def test_join_all_options(self):
        """Join clause, all options
        """
        data_source_builder = TestDataSourceBuilder()
        subject = TextQuery(data_source_builder)

        result = subject.compile_query('#from "source1" #as a1 #filter |filter1| #join-to "source2" #as a2 #where a1.field1 = a2.field2 #exclude-empty #array #field-name test_name')

        self.assertEquals(str(result), "<pos=0, op=SEED, q={0}>,<pos=1, op=JOIN, q={1}, rels=[[field1 == field2]], exclude_empty=True, field_name=test_name, array=True>".format("<uri=source1, filter=filter1>", "<uri=source2, filter=None>"))

    def test_pivot(self):
        """Pivot clause
        """
        data_source_builder = TestDataSourceBuilder()
        subject = TextQuery(data_source_builder)

        result = subject.compile_query('#from "source1" #as a1 #filter |filter1| #pivot-to "source2" #as a2 #where a1.field1 = a2.field2')

        self.assertEquals(str(result), "<pos=0, op=SEED, q={0}>,<pos=1, op=PIVOT, q={1}, rels=[[field1 == field2]]>".format("<uri=source1, filter=filter1>", "<uri=source2, filter=None>"))

    def test_pivot_filter(self):
        """Pivot clause with filter
        """
        data_source_builder = TestDataSourceBuilder()
        subject = TextQuery(data_source_builder)

        result = subject.compile_query('#from "source1" #as a1 #filter |filter1| #pivot-to "source2" #as a2 #filter |filter2| #where a1.field1 = a2.field2')

        self.assertEquals(str(result), "<pos=0, op=SEED, q={0}>,<pos=1, op=PIVOT, q={1}, rels=[[field1 == field2]]>".format("<uri=source1, filter=filter1>", "<uri=source2, filter=filter2>"))

    def test_compound_expression_and(self):
        """Compound expression - 2 and'ed expressions
        """
        data_source_builder = TestDataSourceBuilder()
        subject = TextQuery(data_source_builder)

        result = subject.compile_query('#from "source1" #as a1 #filter |filter1| #pivot-to "source2" #as a2 #filter |filter2| #where a1.field1 = a2.field2 and a1.field2 != a2.field3')

        self.assertEquals(str(result), "<pos=0, op=SEED, q={0}>,<pos=1, op=PIVOT, q={1}, rels=[[field1 == field2, field2 != field3]]>".format("<uri=source1, filter=filter1>", "<uri=source2, filter=filter2>"))

    def test_compound_expression_or(self):
        """Compound expression - 2 or'ed expressions
        """
        data_source_builder = TestDataSourceBuilder()
        subject = TextQuery(data_source_builder)

        result = subject.compile_query('#from "source1" #as a1 #filter |filter1| #pivot-to "source2" #as a2 #filter |filter2| #where a1.field1 = a2.field2 or a1.field2 != a2.field3')

        self.assertEquals(str(result), "<pos=0, op=SEED, q={0}>,<pos=1, op=PIVOT, q={1}, rels=[[field1 == field2], [field2 != field3]]>".format("<uri=source1, filter=filter1>", "<uri=source2, filter=filter2>"))

    def test_compound_expression_and_or(self):
        """Compound expression - and/or compound expression
        """
        data_source_builder = TestDataSourceBuilder()
        subject = TextQuery(data_source_builder)

        result = subject.compile_query('#from "source1" #as a1 #filter |filter1| #pivot-to "source2" #as a2 #filter |filter2| #where a1.field1 = a2.field2 and a1.field2 = a2.field3 or a1.field3 = a2.field4')

        self.assertEquals(str(result), "<pos=0, op=SEED, q={0}>,<pos=1, op=PIVOT, q={1}, rels=[[field1 == field2, field2 == field3], [field3 == field4]]>".format("<uri=source1, filter=filter1>", "<uri=source2, filter=filter2>"))

    def test_compound_expression_grouped(self):
        """Compound expression - and/or compound expression grouped by parens
        """
        data_source_builder = TestDataSourceBuilder()
        subject = TextQuery(data_source_builder)

        result = subject.compile_query('#from "source1" #as a1 #filter |filter1| #pivot-to "source2" #as a2 #filter |filter2| #where a1.field1 = a2.field2 and (a1.field2 = a2.field3 or a1.field3 = a2.field4)')

        self.assertEquals(str(result), "<pos=0, op=SEED, q={0}>,<pos=1, op=PIVOT, q={1}, rels=[[field1 == field2, field2 == field3], [field1 == field2, field3 == field4]]>".format("<uri=source1, filter=filter1>", "<uri=source2, filter=filter2>"))

    def test_compound_expression_grouped_multilevel(self):
        """Compound expression - multiple sub-expressions grouped by nested parens
        """
        data_source_builder = TestDataSourceBuilder()
        subject = TextQuery(data_source_builder)

        result = subject.compile_query('#from "source1" #as a1 #filter |filter1| #pivot-to "source2" #as a2 #filter |filter2| #where (a1.field1 = a2.field2 and (a1.field2 = a2.field3 or (a1.field3 = a2.field4 and a1.field4 = a2.field5)))')

        self.assertEquals(str(result), "<pos=0, op=SEED, q={0}>,<pos=1, op=PIVOT, q={1}, rels=[[field1 == field2, field2 == field3], [field1 == field2, field3 == field4, field4 == field5]]>".format("<uri=source1, filter=filter1>", "<uri=source2, filter=filter2>"))

    def test_literal_escape(self):
        """String literals escape sequences work
        """
        data_source_builder = TestDataSourceBuilder()
        subject = TextQuery(data_source_builder)

        result = subject.compile_query('#from "sou\\\"rce\\\\1" #as a1 #filter |filter1| #pivot-to "sou\\\"rce\\\\2" #as a2 #filter |filter2| #where a1.field1 = a2.field2 or a1.field2 != a2.field3')

        self.assertEquals(str(result), "<pos=0, op=SEED, q={0}>,<pos=1, op=PIVOT, q={1}, rels=[[field1 == field2], [field2 != field3]]>".format("<uri=sou\"rce\\1, filter=filter1>", "<uri=sou\"rce\\2, filter=filter2>"))

    def test_filter_escape(self):
        """Filter escape sequence works
        """
        data_source_builder = TestDataSourceBuilder()
        subject = TextQuery(data_source_builder)

        result = subject.compile_query('#from "source1" #as a1 #filter |filt\\|er1| #pivot-to "source2" #as a2 #filter |filter\\|2| #where a1.field1 = a2.field2 or a1.field2 != a2.field3')

        self.assertEquals(str(result), "<pos=0, op=SEED, q={0}>,<pos=1, op=PIVOT, q={1}, rels=[[field1 == field2], [field2 != field3]]>".format("<uri=source1, filter=filt|er1>", "<uri=source2, filter=filter|2>"))

    def test_filter_optional(self):
        """Filters are optional
        """
        data_source_builder = TestDataSourceBuilder()
        subject = TextQuery(data_source_builder)

        result = subject.compile_query('#from "source1" #as a1 #pivot-to "source2" #as a2 #where a1.field1 = a2.field2 or a1.field2 != a2.field3')

        self.assertEquals(str(result), "<pos=0, op=SEED, q={0}>,<pos=1, op=PIVOT, q={1}, rels=[[field1 == field2], [field2 != field3]]>".format("<uri=source1, filter=None>", "<uri=source2, filter=None>"))

    def test_valid_identifier_chars(self):
        """Identifiers can contain the full range of valid characters
        """
        data_source_builder = TestDataSourceBuilder()
        subject = TextQuery(data_source_builder)

        result = subject.compile_query('#from "source1" #as a_1t@$a? #filter |filter1| #pivot-to "source2" #as a_2t@$a? #filter |filter2| #where a_1t@$a?.field1 = a_2t@$a?.field2 or a_1t@$a?.field2 != a_2t@$a?.field3')

        self.assertEquals(str(result), "<pos=0, op=SEED, q={0}>,<pos=1, op=PIVOT, q={1}, rels=[[field1 == field2], [field2 != field3]]>".format("<uri=source1, filter=filter1>", "<uri=source2, filter=filter2>"))

    def test_malformed_relation_nolhs(self):
        """Relation with no LHS
        """
        data_source_builder = TestDataSourceBuilder()
        subject = TextQuery(data_source_builder)

        with self.assertRaises(TextQueryCompileError):
            subject.compile_query('#from "source1" #as a1 #filter |filter1| #pivot-to "source2" #as a2 #filter |filter2| #where = a2.field2')

    def test_malformed_relation_norhs(self):
        """Relation with no RHS
        """
        data_source_builder = TestDataSourceBuilder()
        subject = TextQuery(data_source_builder)

        with self.assertRaises(TextQueryCompileError):
            subject.compile_query('#from "source1" #as a1 #filter |filter1| #pivot-to "source2" #as a2 #filter |filter2| #where a1.field1 = ')

    def test_malformed_relation_badop(self):
        """Relation with no an invalid comparison operator
        """
        data_source_builder = TestDataSourceBuilder()
        subject = TextQuery(data_source_builder)

        with self.assertRaises(TextQueryCompileError):
            subject.compile_query('#from "source1" #as a1 #filter |filter1| #pivot-to "source2" #as a2 #filter |filter2| #where : a2.field2')

    def test_missing_alias(self):
        """Relation with no an invalid comparison operator
        """
        data_source_builder = TestDataSourceBuilder()
        subject = TextQuery(data_source_builder)

        with self.assertRaises(TextQueryCompileError):
            subject.compile_query('#from "source1" #as a1 #filter |filter1| #pivot-to "source2" #filter |filter2| #where a1.field1 = a2.field2')

    def test_missing_where(self):
        """Relation with no an invalid comparison operator
        """
        data_source_builder = TestDataSourceBuilder()
        subject = TextQuery(data_source_builder)

        with self.assertRaises(TextQueryCompileError):
            subject.compile_query('#from "source1" #as a1 #filter |filter1| #pivot-to "source2" #as a2 #filter |filter2|')

    def test_unresolved_alias(self):
        """An invalid alias causes exception
        """
        data_source_builder = TestDataSourceBuilder()
        subject = TextQuery(data_source_builder)

        with self.assertRaises(TextQueryCompileError):
            subject.compile_query('#from "source1" #as a1 #filter |filter1| #pivot-to "source2" #as a2 #filter |filter2| #where a3.field1 = a2.field2')

    def test_ordered_alias(self):
        """Aliases can be used to express operands in any order
        """
        data_source_builder = TestDataSourceBuilder()
        subject = TextQuery(data_source_builder)

        result = subject.compile_query('#from "source1" #as a1 #filter |filter1| #pivot-to "source2" #as a2 #where a2.field2 = a1.field1')

        self.assertEquals(str(result), "<pos=0, op=SEED, q={0}>,<pos=1, op=PIVOT, q={1}, rels=[[field1 == field2]]>".format("<uri=source1, filter=filter1>", "<uri=source2, filter=None>"))

    def test_failed_datasource(self):
        """Failure to create a data source causes exception
        """
        data_source_builder = TestDataSourceBuilder(True)
        subject = TextQuery(data_source_builder)

        with self.assertRaises(TextQueryCompileError):
            result = subject.compile_query('#from "source1" #as a1 #filter |filter1| #pivot-to "source2" #as a2 #where a2.field2 = a1.field1')


