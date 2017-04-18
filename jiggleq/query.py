"""!
@package jiggleq.query Classes for querying, indexing and filtering Python objects based on a compiled query that associates data sources by field relationships.
"""

from __future__ import print_function
from elasticsearch import Elasticsearch
from elasticsearch import ElasticsearchException
from elasticsearch_dsl import Search
import copy
import logging
import sys
import abc

import jqlog
from relations import F
import relations

class DataSource(object):
    """!
    Abstract data source. Conveniently mirrors the Elasticsearch DSL data acquisition interface.
    """

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def execute(self):
        """!
        Called with the expectation that the data source will load all relevant data in one go.

        @return An iterable object that provides access to the individual result objects from the data source.
        """
        pass

    @abc.abstractmethod
    def scan(self):
        """!
        Called with the expectation that the data source will incrementally load relevant data as the returned object is iterated over.

        This method will only be called by JiggleQ if the client passes @c True as the @c scroll parameter of @c JiggleQ.execute()

        @return An iterable object that provides access to the individual result objects from the data source.
        """
        pass

class ResultShim(object):
    """!
    Step result.
    """
    def __init__(self, data):
        """!
        Constructor.

        @param data: data source object
        """
        self._data = data

    def success(self):
        """!
        Is the result data available?

        @return True if the data from the data source was available, False otherwise
        """
        return True

    def __iter__(self):
        """!
        Get an iterator to the result data.
        """
        return self._data

class ResultHandler(object):
    """!
    Abstract step result handler.
    """

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __call__(self, result, handler_output):
        """!
        Handles a single result object.

        @param result object: The result to handle
        @param handler_output: The location in which to store the handler output, if applicable.
        """
        pass

    @abc.abstractmethod
    def success(self):
        """!
        Has the handler performed its function without error?

        @return @c True if there have been no errors, @c False otherwise
        """
        pass

class ScrollShim(ResultShim):
    """!
    Specialisation of ResultShim for use when using the scan API so that debug output can be emitted.
    """
    def __init__(self, logger, data):
        super(ScrollShim, self).__init__(data)
        logger.debug("Scan session created: {0}".format(str(data)))

class StdoutResultHandler(ResultHandler):
    """!
    Step result handler that simply writes the stringified results to stdout. Intended for use as a query result handler.
    """
    def __call__(self, result, handler_output):
        """!
        Step result handler entry point.

        @param result object: A result to be handled.
        @param handler_output array: Where the handler's output should be placed. Not used by this handler.
        """
        print(str(result))

    def success(self):
        return True

class MatchCallbackProxy(object):
    """!
    Abstract match callback proxy.
    """

    __metaclass__ = abc.ABCMeta

    def __call__(self, instr, subject, match):
        """!
        Callback entry point. Called once for each match.

        @param instr object: Current instruction object.
        @param subject object: The right-hand result object.
        @param match object: The matching left-hand result object.
        """
        pass

class CountingMatchCallbackProxy(MatchCallbackProxy):
    """!
    Proxies a match callback object, tracking counts to the callback.
    """
    def __init__(self, target_callback):
        """!
        Constructor.

        @param target_callback object: The match callback being proxied.
        """
        self.count = 0
        self._target_callback = target_callback

    def __call__(self, instr, subject, match):
        """!
        @see MatchCallbackProxy
        """
        self.count += 1
        self._target_callback(instr, subject, match)

# ref: http://stackoverflow.com/questions/25833613/python-safe-method-to-get-value-of-nested-dictionary
class NestedField(object):
    """!
    An adapter allowing access to a member of any Python object, regardless of how deeply nested in another Python object it may be.
    """
    def __init__(self, obj, field):
        """!
        Constructor.

        @param obj object: The "level zero" object point at which to start when trying to resolve the target member.
        @param field string: A string of the form "level_0_obj.level_1_obj.target_member" to denote the target member to be represented by the NestedField object.
        """
        self.obj = obj
        self.field = field
        self._value = None

    def exists(self):
        """!
        Does the target member specified exist?

        If found, the target member value is cached for use by the @c value() method. After being cached, subsequent calls to @c exists() and @c value() will return the same results as they did on the first call regardless of whether or not the target object has changed. To override this behaviour, call @c clear_cache().

        @return @c True if the target member exists, @c False if it doesn't
        """
        def get_level(obj, field):
            return obj[field]

        if (self._value is None):

            try:
                self._value = reduce(get_level, self.field.split("."), self.obj)
            except KeyError, TypeError:
                return False

        return True

    def value(self):
        """!
        What is the value of the target member specified? Returns @c None if the target member doesn't exist.

        Implicitly calls @c exists(), so the same caching behaviour applies.

        @return @c None if the target member doesn't exist, the value if it does.
        """
        if (self.exists()):
            return self._value
        else:
            return None

    def clear_cache(self):
        """!
        Clears cached target member value, forcing it to be retrieved from the target object on the next call to @c value()
        """
        self._value = None

class IndexResultHandler(object):
    """!
    Indexes results from one query step according to specified index requirements (such as the filter requirements of a subsequent query step).
    """
    def __init__(self, index_conditions, logger):
        """!
        Constructor.
        
        @param index_conditions object: The fields to index and the logic that relates them.
        """
        self.index_conditions = index_conditions
        self._hit_group_count = 0
        self._logger = logger

        self._logger.debug("{0} index condition group(s)".format(len(self.index_conditions)))

    def __call__(self, result, handler_output):
        """!
        Performs the indexing. Each condition group within the index conditions specifies the name of fields that must be indexed together as AND'ed sub-expressions.

        @param result object: A single result to index
        @param handler_output object: The object index
        """
        if (len(handler_output) == 0):
            for cond_group_index in xrange(len(self.index_conditions)):
                handler_output.append({F.OP_EQ : {}, F.OP_NE : {}})

        cond_group_index = 0
        for cond_group in self.index_conditions:
            result_keys = {F.OP_EQ : [], F.OP_NE : []}
            cond_count = 0
            for cond in cond_group:
                field = NestedField(result, cond.left_field)
                if (field.exists()):

                    result_key = (cond_count, cond.lhs_proxy(cond.left_field, field.value()))
                    result_keys[cond.op].append(result_key)

                    cond_count += 1
                else:
                    break

            if (cond_count == len(cond_group)):
                self._hit_group_count += 1 # The object satisfies the condition group field dependencies
                index_key_eq = tuple(result_keys[F.OP_EQ])

                if (len(index_key_eq) > 0):
                    if (index_key_eq not in handler_output[cond_group_index][F.OP_EQ]):
                        handler_output[cond_group_index][F.OP_EQ][index_key_eq] = []

                    handler_output[cond_group_index][F.OP_EQ][index_key_eq].append(result)

                index_keys_ne = result_keys[F.OP_NE]
                for index_key_ne in index_keys_ne:
                    if (index_key_ne not in handler_output[cond_group_index][F.OP_NE]):
                        handler_output[cond_group_index][F.OP_NE][index_key_ne] = []

                    handler_output[cond_group_index][F.OP_NE][index_key_ne].append(result)
                
            cond_group_index += 1

    def success(self):
        """!
        Success is defined as at least one object satisfying the field requirements of the index conditions.

        @return @c True if the field requirements are satisfied for one or more objects, @c False otherwise
        """
        self._logger.debug("{0} possibly-related field value(s)".format(self._hit_group_count))
        return (self._hit_group_count > 0)

class JiggleQ(object):
    """!
    @brief A JiggleQ query.

    A query is made up of one or more steps. The first step is always a "seed" step, consisting only of a data source. A data source is simply an object exposing a particular interface that JiggleQ can use in order to receive data in the form of Python objects.
    
    Subsequent steps will be either "join" or "pivot" steps, and are associated with both a data source and one or more relationship conditions. A relationship condition is a mapping between two fields: one in the results from the previous step's data source (the "right-hand side") and one in the results from the current step's data source (the "left-hand side"). Currently, fields can only be related by equality or inequality.

    The results of a join step are the objects from that step's data source with objects from the previous step joined to them where the relationship conditions hold.
    
    The results of a pivot step are the objects from the step's data source for which the relationship conditions hold for at least one object in the previous step's results (in the case of equality conditions) or for which the relationship conditions hold for all objects in the previous step's results (in the case of inequality conditions).

    @see jiggleq.relations.F
    @see DataSource
    """

    OP_SEED = 0
    OP_PIVOT = 1
    OP_JOIN = 2

    def __init__(self, search):
        """!
        Constructor.

        @param search object: Query seed data source - data for the first step of the query.
        """
        self._instruction_set = {
            JiggleQ.OP_SEED : {"name" : "seed", "after" : None, "match_callback" : None},
            JiggleQ.OP_PIVOT : {"name" : "pivot", "after" : self._stage_after, "match_callback" : None},
            JiggleQ.OP_JOIN : {"name" : "join", "after" : self._stage_after, "match_callback" : self._join_match_callback}
        }

        # Create a logging object
        self._logger = jqlog.JqLogger.get()

        self._results = []

        self._instructions = []
        self._instructions.append({"op":JiggleQ.OP_SEED, "conditions":None, "q":search, "conjunctions":None})
        self._result_handler = StdoutResultHandler()
        self.result = None

    def result_handler(self, handler):
        """!
        Sets the query's result handler.

        @param handler object: 

        @see
        """
        self._result_handler = handler

    def logger(self):
        return self._logger

    def join_to(self, data_source, rel, field=None, array=False, exclude_empty_joins=False):
        """!
        Adds a new step to the query that joins the results of the previous step with the results of the added step's data source, when the field relationships specified hold.

        @param data_source object: The step's data source object. Must implement the interface defined by DataSource.
        @param rel jiggleq.relations.F: The field relationships.
        @param field string: The name to give the joined field in result objects.
        @param array boolean: Whether or not joined fields should support the joining of multiple objects.
        @param exclude_empty_joins boolean: Whether or not left-hand results that don't have anything joined to them should be discarded.

        @return A JiggleQ object representing the query so far
        """
        target_conds = relations.TargetConditions(rel.tree)
        self._instructions[-1]["conjunctions"] = target_conds.conjunctions
        self._instructions.append({"op":JiggleQ.OP_JOIN, "exclude_empty_matches":exclude_empty_joins, "field":field, "array":array, "conditions":target_conds, "q":data_source, "conjunctions":[]})
        return self

    def pivot_to(self, data_source, rel):
        """!
        Adds a new step to the query that selects only the results of the data source whose fields are related to the previous step's results according to the relationships specified.

        @param data_source object: The step's data source object. Must implement the interface defined by DataSource.
        @param rel jiggleq.relations.F: The field relationships.

        @return A JiggleQ object representing the query so far
        """
        target_conds = relations.TargetConditions(rel.tree)
        self._instructions[-1]["conjunctions"] = target_conds.conjunctions
        self._instructions.append({"op":JiggleQ.OP_PIVOT, "conditions":target_conds, "q":data_source, "conjunctions":[]})
        return self

    def _filter_and_store(self, instr, response, filter_conditions, result_handler):
        """!
        Uses the previous query step's index to filter results and discard those that don't satisfy the filter conditions.

        Index keys for each right-hand result are created on-the-fly for lookup in the previous step's index.

        O(n) worst-case time complexity for conditions that contain only equality relationships, where n = the number of results. O(n*n) worst-case time complexity for conditions that contain inequality conditions, where n = the number of results.

        The behaviour of this method depends heavily on whether or not the filter matches (i.e. results that satisfy filter conditions) need to be sent to a match callback. If they don't, substantial shortcuts are taken (such as to avoid iterating over all inequality matches). Pivoting does not require filter matches to be sent to a match callback, joining does.

        Once a right-hand result is known to match the filter conditions, it is passed to @c result_handler. This will either index the result ahead of the next query step, or if there are no further query steps, will pass the result to the client-supplied result handler.

        @param instr object: Current query instruction
        @param response object: Data source response object/result set
        @param filter_conditions object: The conditions - field names and relationships - that must be used to filter the results
        @param result_handler object: The handler that is to process the filtered results
        """
        self._results.append([])

        for h in response:
            result = h.to_dict()

            # Filter

            cond_group_index = 0
            cond_group_satisfied = False
            for cond_group in filter_conditions:
                try:
                    cond_count = 0
                    filter_keys_eq = []
                    filter_keys_ne = []
                    eq_ne_index = {}
                    for cond in cond_group:
                        field = NestedField(result, cond.right_field)
                        if (field.exists()):
                            filter_key = (cond_count, cond.rhs_proxy(cond.right_field, field.value()))
                            if (cond.op == F.OP_EQ):
                                filter_keys_eq.append(filter_key)
                                cond_count += 1
                            elif (cond.op == F.OP_NE):
                                filter_keys_ne.append(filter_key)
                                cond_count += 1
                        else:
                            break

                    if (cond_count == len(cond_group)):
                        # All fields in group present
                        filter_key_eq = tuple(filter_keys_eq)

                        eq_matches = None
                        try:
                            eq_matches = self._results[-2][cond_group_index][F.OP_EQ][filter_key_eq]
                        except KeyError:
                            eq_matches = []

                        match_callback = None if (self._instruction_set[instr["op"]]["match_callback"] is None) else CountingMatchCallbackProxy(self._instruction_set[instr["op"]]["match_callback"])

                        ne_matches = []
                        for filter_key_ne in filter_keys_ne:
                            try:
                                filter_matches = self._results[-2][cond_group_index][F.OP_NE][tuple(filter_key_ne)]
                                for filter_match in filter_matches:
                                    ne_matches.append(filter_match)
                                    if (match_callback is None):
                                        # No need to record all the inequality matches unless the left-hand results are required
                                        break
                            except KeyError:
                                pass

                        self._logger.debug("Found {0} inequality match(es)".format(len(ne_matches)))

                        # If the left-hand results are not required, right-hand results can be easily excluded based on the filter key match counts
                        if (match_callback is None):
                            if (len(filter_keys_ne) > 0):
                                if (len(ne_matches) > 0):
                                    continue

                            if (len(filter_keys_eq) > 0):
                                if (len(eq_matches) == 0):
                                    continue

                            cond_group_satisfied = True

                        else:

                            def index_ne(ne, indexed):
                                for ne_match in ne:
                                    self._logger.debug("Added NE result to index: {0} as {1}".format(ne_match, id(ne_match)))
                                    indexed[id(ne_match)] = None

                            if ((len(filter_keys_eq) > 0) and (len(filter_keys_ne) > 0)):
                                # If there are both equality and inequality conditions, the desired matches are the intersection of the equality match set with the complement of the inequality match set
                                # That is, each equality match must be tested for the inequality conditions

                                if (cond_group_index not in eq_ne_index):
                                    eq_ne_index[cond_group_index] = {}
                                    index_ne(ne_matches, eq_ne_index[cond_group_index])

                                for eq_match in eq_matches:
                                    if (id(eq_match) not in eq_ne_index[cond_group_index]):
                                        match_callback(instr, result, eq_match)

                            elif ((len(filter_keys_eq) == 0) and (len(filter_keys_ne) > 0)):
                                # If there are no equality conditions, the desired matches are the complement of the inequality match set

                                if (cond_group_index not in eq_ne_index):
                                    eq_ne_index[cond_group_index] = {}
                                    index_ne(ne_matches, eq_ne_index[cond_group_index])

                                for possible_match_key, possible_match_array in self._results[-2][cond_group_index][F.OP_NE].iteritems():
                                    for match_value in possible_match_array:
                                        if (id(match_value) not in eq_ne_index[cond_group_index]):
                                            match_callback(instr, result, match_value)
                                        else:
                                            break

                            elif ((len(filter_keys_eq) > 0) and (len(filter_keys_ne) == 0)):
                                # If there are only equality conditions, the desired matches are simply all the equality matches
                                # Iteration over all matches is only required if there's a match callback
                                for eq_match in eq_matches:
                                    match_callback(instr, result, eq_match)

                            cond_group_satisfied = ((instr["exclude_empty_matches"]) and (match_callback.count > 0)) or (not instr["exclude_empty_matches"])
                            if (cond_group_satisfied):
                                break

                finally:
                    cond_group_index += 1

            # Index or finalise
            if ((cond_group_satisfied) or (len(filter_conditions) == 0)):
                result_handler(result, self._results[-1])

    def _process_response(self, instr, response, index_conditions, filter_conditions):
        """!
        Applies appropriate logic to a data source response based on the query step and success of the data source request.

        @param instr object: Current instruction object
        @param response object: The data source's response
        @param index_conditions: Conditions against which the response should be indexed
        @param filter_conditions: Conditions against which the response should be filtered

        @return @c response if successful or @c None otherwise 
        """
        if (response.success()):
            if (hasattr(response, "took")):
                self._logger.info("Query successful: took {0}ms to find {1} hit(s)".format(response.took, response.hits.total))

            handler = None
            if (len(index_conditions) > 0):
                handler = IndexResultHandler(index_conditions, self._logger)
            else:
                handler = self._result_handler
                    
            self._filter_and_store(instr, response, filter_conditions, handler)

            if (handler.success()):
                return response
            else:
                return None

        else:
            self._logger.error("Query failed, aborting...")
            return None

    def _stage_after(self, instr):
        """!
        Delete's the previous query step's results. Used to discard data that's no longer needed. Important for multi-step queries that yield large result sets.

        @param instr object: Current instruction object
        """
        del self._results[0]

    def _join_match_callback(self, instr, subject, match):
        """!
        The callback invoked when a right-hand result is determined to be eligible for joining to a left-hand result.

        If the join step has been specified to support multiple results, the joined data will be a list of joined objects. Otherwise, the joined data will be the first single right-hand object found to be eligible for the join.

        @param instr object: The current instruction object.
        @param subject object: The right-hand result.
        @param match: The left-hand result.
        """
        field_name = instr["field"]
        if (field_name is None):
            field_name = "joined_data"

        array_required = instr["array"]
        if (array_required):
            if (field_name not in subject):
                subject[field_name] = []
                
            if (type(subject[field_name]) is list):
                subject[field_name].append(match)
            else:
                self._logger.warn("Couldn't join record to {0} because a non-array field called {1} already exists".format(str(subject), field_name))
        else:
            if (field_name in subject):
                self._logger.warn("Couldn't join record to {0} because a field called {1} already exists".format(str(subject), field_name))
            else:
                subject[field_name] = match

    def _execute_instruction(self, instr):
        """!
        Request data from the data source associated with the instruction and process the response.

        @param instr object: Current instruction object.

        @return @c True if the instruction executed successfully, @c False otherwise
        """
        response = None

        try:
            response = self._process_response(instr, ScrollShim(self._logger, instr["q"].scan()) if instr["scroll"] else instr["q"].execute(), [] if (instr["conjunctions"] is None) else instr["conjunctions"], [] if (instr["conditions"] is None) else instr["conditions"].conjunctions)
        except ElasticsearchException as e:
            self._logger.error("Elasticsearch error: {0}".format(str(e)))
            response = None
        except Exception as e:
            self._logger.error("Error: {0}".format(str(e)))
            response = None

        if (response is None):
            return False
        else:
            return True

    def execute(self, scroll=False):
        """!
        Execute the query. Runs each query step from left to right.

        @param scroll boolean: If @c True, the data source's @c scan() method will be used to retrieve results. If @c False, the data source's @c execute() method will be used instead.

        @see DataSource
        """
        try:
            self.result = {}
            stage_index = 0
            for instr in self._instructions:
                self._logger.debug("<Step {0}> Running {1} query...".format(stage_index, self._instruction_set[instr["op"]]["name"]))
                instr["scroll"] = scroll
                if (not self._execute_instruction(instr)):
                    return False
                else:
                    after_event = self._instruction_set[instr["op"]]["after"]
                    if (after_event is not None):
                        after_event(instr)

                stage_index += 1
        except Exception as e:
            self._logger.error("Query failed: {0}".format(e))
            return False

        return True

if __name__ == "__main__":
    c = Elasticsearch(hosts=["127.0.0.1:9200"])
    q1 = Search(using=c, index="requests").query("match", method="CONNECT")
    q2 = Search(using=c, index="clients").query("match_all")
    q3 = Search(using=c, index="customers").query("match_all")
    q4 = Search(using=c, index="requests").query("match_all")
    s = JiggleQ(q1).pivot_to(q2, F("src_ip") == F("ip")).join_to(q3, (F("country") != F("personal.location.country")) & (F("customer_id") != F("id")), array=True)
    #s = JiggleQ(q1).pivot_to(q2, F("src_ip") == F("ip")).join_to(q3, (F("customer_id") != F("id")), array=True)
    #s = JiggleQ(q1).pivot_to(q2, F("src_ip") == F("ip")).join_to(q3, (F("country") == F("personal.location.country")), array=True)
    s.logger().setLevel(logging.DEBUG)
    s.execute(scroll=True)

