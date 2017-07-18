# -*- coding: utf-8 -*-

"""!
@package weaveq.parser Classes for parsing and compiling textual queries to WeaveQ objects.
"""

from pyparsing import ParseResults, Keyword, CaselessKeyword, Word, alphanums, Optional, CaselessLiteral, Literal, QuotedString, infixNotation, ZeroOrMore, oneOf, OneOrMore, stringEnd, opAssoc, ParseBaseException, Group
import abc
import copy

import query
from relations import F
from query import WeaveQ
from wqexception import TextQueryCompileError

class DataSourceBuilder(object):
    """!
    @brief Abstract data source builder functor.

    Defines an interface for client callbacks to be requested to build weaveq.query.DataSource objects as a result of query parsing. Note that builders don't have to inherit from this class, but must at least expose the interface it defines.
    """

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __call__(self, source_uri, filter_string):
        """!
        Called when a weaveq.query.DataSource object needs to be built in order to run a WeaveQ query.

        @param source_uri: the URI describing the source of the data (specified by the string literal preceding the @c #as statement in the WeaveQ query)
        @param filter_string: the filter specified to be applied to the data at source (specified by the text within pipe characters in the @c #filter clause), or @c None if no filter was specified

        @return An object either of the type weaveq.query.DataSource or exposing a compatible interface (e.g. an Elasticsearch DSL query), or @c None if building failed

        @see weaveq.query.DataSource
        """
        pass

class TextQuery(object):
    """!
    @brief Textual query parser and compiler.

    Compiles a textual query into a WeaveQ object. Data source URIs are specified in the query text, so the client of this class must supply a DataSourceBuilder object for the conversion of these URIs to weaveq.query.DataSource objects.
    """

    STEP_TYPE_SEED = 0
    STEP_TYPE_PIVOT = 1
    STEP_TYPE_JOIN = 2

    NODE_TYPE_ATOM = 0
    NODE_TYPE_COMPOUND = 1
    NODE_TYPE_OPERATOR = 2

    def __init__(self, data_source_builder):
        """!
        Constructor.

        @param data_source_builder: converts data source URIs to weaveq.query.DataSource objects

        @see weaveq.query.DataSource
        """
        self._identifier = Word(alphanums + "_" + "." + "@" + "$" + "?")
        self._string_literal = QuotedString('"', escChar='\\')
        self._field_equality_op = oneOf("= !=")
        self._field_relationship = Group(self._identifier - self._field_equality_op - self._identifier)
        self._field_expr = infixNotation(self._field_relationship, [(Keyword("and"), 2, opAssoc.LEFT), (Keyword("or"), 2, opAssoc.LEFT),]).setResultsName("field_relations")
        self._where_clause = Keyword("#where") - self._field_expr
        self._filter_expr = QuotedString(quoteChar="|", escChar="\\")
        self._source_spec = self._string_literal.setResultsName("source_uri") - Keyword("#as") - self._identifier.setResultsName("source_alias") - Optional(Keyword("#filter") - self._filter_expr.setResultsName("source_filter_string"))
        self._pivot_clause = Keyword("#pivot-to").setResultsName("step_action") - self._source_spec - self._where_clause
        self._join_options = (Keyword("#field-name") - self._identifier.setResultsName("field_name")) | Keyword("#exclude-empty").setResultsName("exclude_empty") | Keyword("#array").setResultsName("array")
        self._join_clause = Keyword("#join-to").setResultsName("step_action") - self._source_spec - self._where_clause - ZeroOrMore(self._join_options)
        self._process_clause = self._pivot_clause | self._join_clause
        self._seed_clause = Keyword("#from").setResultsName("step_action") - self._source_spec
        
        self._seed_clause.setParseAction(self._create_step)
        self._pivot_clause.setParseAction(self._create_step)
        self._join_clause.setParseAction(self._create_step)

        self._query = self._seed_clause - OneOrMore(self._process_clause)

        self.data_source_builder = data_source_builder

        self._parsed_query = []
        self._source_by_alias = {}

    def _order_operands(self, operands):
        """!
        Checks the order of field expression operands and re-orders them, if necessary.

        @param operands: a list containing the two comparison operands to order

        @return a list of two operands in the correct order
        """
        name_l = operands[0]
        name_r = operands[1]

        left_indx = -1
        right_indx = -1

        try:
            left_indx = self._source_by_alias[name_l[0:name_l.index(".")]]
        except KeyError:
            raise TextQueryCompileError("The alias {0} is not assigned to a source".format(name_l))

        try:
            right_indx = self._source_by_alias[name_r[0:name_r.index(".")]]
        except KeyError:
            raise TextQueryCompileError("The alias {0} is not assigned to a source".format(name_r))

        if (left_indx == len(self._parsed_query) - 1):
            if (right_indx != len(self._parsed_query) - 2):
                raise TextQueryCompileError("The alias {0} is out of scope".format(name_r))

            operands[0] = name_r[name_r.index(".")+1:]
            operands[1] = name_l[name_l.index(".")+1:]
        elif (left_indx != len(self._parsed_query) - 2):
            raise TextQueryCompileError("The alias {0} is out of scope".format(name_l))
        elif (right_indx != len(self._parsed_query) - 1):
            raise TextQueryCompileError("The alias {0} is out of scope".format(name_r))
        else:
            operands[0] = name_l[name_l.index(".")+1:]
            operands[1] = name_r[name_r.index(".")+1:]

        return operands

    def _compile_sub_expr(self, lhs, op, rhs):
        """!
        Converts a pair of operands and comparison operator into a weaveq.relations.F object

        @param lhs: left-hand side operand as a fully-qualified alias and field name
        @param op: comparison operator as the string representation of the operator
        @param rhs: right-hand side operand as a fully-qualified alias and field name

        @return the weaveq.relations.F result
        """
        sub_expr = None
        operands = self._order_operands([lhs, rhs])
        if (op == '='):
            sub_expr = (F(operands[0]) == F(operands[1]))
        elif (op == '!='):
            sub_expr = (F(operands[0]) != F(operands[1]))

        return sub_expr

    def _node_type(self, node):
        """!
        Determines the type of a node from PyParsing's infix notation type parsed out of the where clause

        @param node: the node whose type will be determined

        @return an integer denoting the type of the node

        @see TextQuery.NODE_TYPE_ATOM
        @see TextQuery.NODE_TYPE_COMPOUND
        @see TextQuery.NODE_TYPE_OPERATOR
        """
        if ((len(node) == 3) and (isinstance(node, ParseResults))):
            if (isinstance(node[0], str) and isinstance(node[1], str) and isinstance(node[2], str)):
                return TextQuery.NODE_TYPE_ATOM
            else:
                return TextQuery.NODE_TYPE_COMPOUND
        elif (isinstance(node, str)):
            return TextQuery.NODE_TYPE_OPERATOR
        else:
            return TextQuery.NODE_TYPE_COMPOUND

    def _reduce_stack(self, expr_stack):
        """!
        Performs the operation described by an expression stack of the form [operand] [logical-operator] [operand] and stores the result as the single element in a new expression stack

        @return the expression stack after reduction
        """
        if (len(expr_stack) == 3):
            if (expr_stack[-2] == "and"):
                expr_stack = [(expr_stack[-3] & expr_stack[-1])]
            elif (expr_stack[-2] == "or"):
                expr_stack = [(expr_stack[-3] | expr_stack[-1])]

        return expr_stack

    def _transform_field_relations(self, rel, expr_stack = None):
        """!
        Converts a nested list representation of a syntax tree, as produced by PyParsing, into the corresponding weaveq.relations.F representation.

        @param rel: infix notation output from PyParsing
        @param expr_stack: current expression stack

        @return 1-element expression stack containing the results of the given expression
        """
        if (expr_stack is None):
            expr_stack = []

        node_type = self._node_type(rel)

        if (node_type == TextQuery.NODE_TYPE_ATOM):
            expr_stack.append(self._compile_sub_expr(rel[0], rel[1], rel[2]))
            expr_stack = self._reduce_stack(expr_stack)
        elif (node_type == TextQuery.NODE_TYPE_OPERATOR):
            expr_stack.append(rel)
        elif (node_type == TextQuery.NODE_TYPE_COMPOUND):
            for expr in rel:
                expr_stack.append(self._transform_field_relations(expr)[-1])
                expr_stack = self._reduce_stack(expr_stack)

        return expr_stack

    def _create_step(self, tokens):
        """!
        Callback during parsing to produce an intermediate representation of a weaveq.query.WeaveQ

        @param tokens: tokens from PyParsing
        """
        field_relations = []
        if (tokens.step_action == "#from"):
            self._parsed_query.append({"type" : TextQuery.STEP_TYPE_SEED})
        elif (tokens.step_action == "#pivot-to"):
            field_relations = tokens.field_relations
            self._parsed_query.append({"type" : TextQuery.STEP_TYPE_PIVOT})
        elif (tokens.step_action == "#join-to"):
            field_relations = tokens.field_relations
            self._parsed_query.append({"type" : TextQuery.STEP_TYPE_JOIN})
            if (len(tokens.field_name) > 0):
                self._parsed_query[-1]["field_name"] = tokens.field_name
            else:
                self._parsed_query[-1]["field_name"] = None
            if (len(tokens.exclude_empty) > 0):
                self._parsed_query[-1]["exclude_empty"] = True
            else:
                self._parsed_query[-1]["exclude_empty"] = False
            if (len(tokens.array) > 0):
                self._parsed_query[-1]["array"] = True
            else:
                self._parsed_query[-1]["array"] = False
        else:
            raise TextQueryCompileError("Unexpected query step action: {0}".format(tokens.step_action))

        filter_string = tokens.source_filter_string if len(tokens.source_filter_string) > 0 else None
        data_source = self.data_source_builder(tokens.source_uri, filter_string)
        if (data_source is None):
            raise TextQueryCompileError("Failed to build data source for {0}: builder failed".format(tokens.source_uri))

        self._parsed_query[-1]["source_uri"] = tokens.source_uri
        self._parsed_query[-1]["source_filter_string"] = filter_string
        self._parsed_query[-1]["source_alias"] = tokens.source_alias
        self._source_by_alias[tokens.source_alias] = len(self._parsed_query) - 1
        self._parsed_query[-1]["data_source"] = data_source

        if (len(field_relations) == 0):
            self._parsed_query[-1]["relations"] = None
            self._parsed_query[-1]["field_expression"] = None
        else:
            self._parsed_query[-1]["relations"] = field_relations

            try:
                self._parsed_query[-1]["field_expression"] = self._transform_field_relations(self._parsed_query[-1]["relations"])[-1]
            except Exception as e:
                raise TextQueryCompileError("Error transforming field relations: {0}".format(str(e)))

    def compile_query(self, query_string):
        """!
        Compiles a textual query into a weaveq.query.WeaveQ object.

        @param query_string: string containing textual query

        @return weaveq.query.WeaveQ result
        """

        # Parse the text query

        self._parsed_query = []
        self._source_by_alias = {}

        try:
            self._parse(query_string)
        except ParseBaseException as e:
            raise TextQueryCompileError("Parse error at col {0}: {1}".format(e.column, e.msg))

        # Create a query object from the parse results
        result = None
        for parse_result in self._parsed_query:
            if (parse_result["type"] == TextQuery.STEP_TYPE_SEED):
                if (result is not None):
                    raise TextQueryCompileError("Unexpected seed query out of sequence")

                result = WeaveQ(parse_result["data_source"])
            else:
                if (result is None):
                    raise TextQueryCompileError("Unexpected non-seed query out of sequence")

                if (parse_result["type"] == TextQuery.STEP_TYPE_PIVOT):
                    result = result.pivot_to(parse_result["data_source"], parse_result["field_expression"])
                elif (parse_result["type"] == TextQuery.STEP_TYPE_JOIN):
                    result = result.join_to(parse_result["data_source"], parse_result["field_expression"], field=parse_result["field_name"], array=parse_result["array"], exclude_empty_joins=parse_result["exclude_empty"])

        return result

    def _parse(self, query_string):
        (self._query + stringEnd).parseString(query_string)

