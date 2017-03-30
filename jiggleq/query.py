"""@package query
Classes for querying, indexing and filtering Python objects based on a query compiled from data sources associated by field relationships.
"""

from __future__ import print_function
from elasticsearch import Elasticsearch
from elasticsearch import ElasticsearchException
from elasticsearch_dsl import Search
import copy
import logging
import sys

import jqlog
from relations import F
import relations

class ResultShim(object):
    """Step result.
    """
    def __init__(self, data):
        """Constructor. data is the data source object.
        """
        self._data = data

    def success(self):
        """Is the result data available?
        """
        return True

    def __iter__(self):
        """Get a data iterator.
        """
        return self._data

class ScrollShim(ResultShim):
    """Specialisation of ResultShim for use when using the Elastic scroll API so that debug output can be emitted.
    """
    def __init__(self, logger, data):
        super(ScrollShim, self).__init__(data)
        logger.debug("Scroll session created: {0}".format(str(data)))

class StdoutResultHandler(object):
    """Query result handler that simply writes the stringified results to stdout.
    """
    def __call__(self, result, handler_output):
        print(str(result))

    def success(self):
        return True

class CountingMatchCallbackProxy(object):
    """Proxies a match callback object, tracking counts to the callback.
    """
    def __init__(self, target_callback):
        """Constructor.
        """
        self.count = 0
        self._target_callback = target_callback

    def __call__(self, instr, subject, match):
        """Callback entry point.
        """
        self.count += 1
        self._target_callback(instr, subject, match)

# ref: http://stackoverflow.com/questions/25833613/python-safe-method-to-get-value-of-nested-dictionary
class NestedField(object):
    """An adapter allowing access to a member of any Python object, regardless of how deeply nested in another Python object it may be. The "level zero" object point at which to start when trying to resolve the target member is specified by obj, while a string of the form "level_0_obj.level_1_obj.target_member" is specified by field to denote the target member to be represented by the NestedField object.
    """
    def __init__(self, obj, field):
        self.obj = obj
        self.field = field
        self._value = None

    def exists(self):
        """Does the target member specified exist?

        If found, the target member value is cached for use by the value() method. After being cached, subsequent calls to exist() and value() will return the same results as they did on the first call regardless of whether or not the target object has changed. To override this behaviour, call clear_cache().
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
        """What is the value of the target member specified? Returns None if the target member doesn't exist.

        Implicitly calls exist, so the same caching behaviour applies.
        """
        if (self.exists()):
            return self._value
        else:
            return None

    def clear_cache(self):
        """Clears cached target member value.
        """
        self._value = None

class IndexResultHandler(object):
    """Indexes results from one query step according to specified index requirements (e.g. the filter requirements of a subsequent query step).
    """
    def __init__(self, index_conditions, logger):
        """Constructor. Index conditions specify the fields to index and the logic that relates them.
        """
        self.index_conditions = index_conditions
        self._hit_group_count = 0
        self._logger = logger

        self._logger.debug("{0} index condition group(s)".format(len(self.index_conditions)))

    def __call__(self, result, handler_output):
        """Performs the indexing. Each condition group within the index conditions specifies the name of fields that must be indexed together as AND'ed sub-expressions.
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

                    self._logger.debug("Result key prepared for condition group index: {0} => ({1} {2} {3})".format(cond_group_index, result_key[0], F.op_str(cond.op), result_key[1]))

                    cond_count += 1
                else:
                    self._logger.debug("Field {0} absent from results - skipping indexing of condition group {1}".format(cond.left_field, str(cond)))
                    break

            if (cond_count == len(cond_group)):
                self._logger.debug("All required fields found in hit, indexing {0} field(s)...".format(len(result_keys)))

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
                
                self._logger.debug("Index for group {0}: {1} => ({2}), {3} => ({4})".format(cond_group_index, F.op_str(F.OP_EQ), handler_output[cond_group_index][F.OP_EQ], F.op_str(F.OP_NE), handler_output[cond_group_index][F.OP_NE]))

            cond_group_index += 1

    def success(self):
        """Success is defined as at least one object satisfying the field requirements of the index conditions.
        """
        self._logger.debug("{0} possibly-related field value(s)".format(self._hit_group_count))
        return (self._hit_group_count > 0)

class JiggleQ(object):
    """A JiggleQ query.
    """

    OP_SEED = 0
    OP_PIVOT = 1
    OP_JOIN = 2

    def __init__(self, search):
        """Constructor. Sets up an instruction set to associate callbacks with query actions.
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
        self._result_handler = handler

    def logger(self):
        return self._logger

    def join_to(self, search, rel, field=None, array=False, exclude_empty_joins=False):
        """Adds a new step to the query that joins the results of the previous step with the results of search when the field relationships specified hold.

        The name of the object member under which the joined results will reside can be specified using field. A default name of "joined_data" will be used otherwise.

        array can be set to True to ensure the field containing joined results is a list, allowing multiple objects to be joined to the same parent object.

        exclude_empty_joins can be set to True to discard objects produced by search that don't end up having anything joined to them.
        """
        target_conds = relations.TargetConditions(rel.tree)
        self._instructions[-1]["conjunctions"] = target_conds.conjunctions
        self._instructions.append({"op":JiggleQ.OP_JOIN, "exclude_empty_matches":exclude_empty_joins, "field":field, "array":array, "conditions":target_conds, "q":search, "conjunctions":[]})
        return self

    def pivot_to(self, search, rel):
        """Adds a new step to the query that selects only the results of search whose fields are related to the previous step's results according to the relationships specified.
        """
        target_conds = relations.TargetConditions(rel.tree)
        self._instructions[-1]["conjunctions"] = target_conds.conjunctions
        self._instructions.append({"op":JiggleQ.OP_PIVOT, "conditions":target_conds, "q":search, "conjunctions":[]})
        return self

    def _filter_and_store(self, instr, response, filter_conditions, result_handler):
        """Uses the previous query step's index to filter results and discard those that don't satisfy the filter conditions.

        Index keys for each right-hand result are created on-the-fly for lookup in the previous step's index.

        O(n) worst-case time complexity for conditions that contain only equality relationships, where n = the number of results. O(n*n) worst-case time complexity for conditions that contain inequality conditions, where n = the number of results.

        The behaviour of this method depends heavily on whether or not the filter matches (i.e. left-hand results that satisfy filter conditions) need to be sent to a match callback. If they don't, substantial shortcuts are taken (e.g. to avoid iterating over all inequality matches). Pivoting does not require filter matches to be sent to a match callback, joining does.

        Once a right-hand result is known to match the filter conditions, it is passed to result_handler. This will either index the result ahead of the next query step, or if there are no further query steps, will pass the result to the client-supplied result handler.
        """
        self._logger.debug("{0} filter condition group(s)".format(len(filter_conditions)))

        self._results.append([])

        for h in response:
            result = h.to_dict()

            # Filter

            cond_group_index = 0
            cond_group_satisfied = False
            for cond_group in filter_conditions:
                self._logger.debug("Evaluating condition group {0}".format(cond_group))
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
                            self._logger.debug("Field {0} doesn't exist in right-hand result".format(str(cond.right_field)))
                            break

                    if (cond_count == len(cond_group)):
                        # All fields in group present
                        self._logger.debug("All field(s) required in group {0} present: {1} equality, {2} inequality".format(cond_group_index, len(filter_keys_eq), len(filter_keys_ne)))
                        filter_key_eq = tuple(filter_keys_eq)

                        eq_matches = None
                        try:
                            eq_matches = self._results[-2][cond_group_index][F.OP_EQ][filter_key_eq]
                        except KeyError:
                            eq_matches = []

                        self._logger.debug("Found {0} equality match(es)".format(len(eq_matches)))

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
                                    self._logger.debug("Combination of equality and inequality match conditions requires further indexing for condition group {0}".format(cond_group_index))

                                    eq_ne_index[cond_group_index] = {}
                                    index_ne(ne_matches, eq_ne_index[cond_group_index])

                                self._logger.debug("NE matches: {0} on {1}, EQ matches: {2} on {3}".format(ne_matches, filter_keys_ne, eq_matches, filter_keys_eq))

                                for eq_match in eq_matches:
                                    if (id(eq_match) not in eq_ne_index[cond_group_index]):
                                        match_callback(instr, result, eq_match)
                                        self._logger.debug("NE field relationship condition(s) hold for {0}, indexed as {1}".format(eq_match, id(eq_match)))
                                    else:
                                        self._logger.debug("NE field relationship condition(s) don't hold for {0}, indexed as {1}".format(eq_match, id(eq_match)))

                            elif ((len(filter_keys_eq) == 0) and (len(filter_keys_ne) > 0)):
                                # If there are no equality conditions, the desired matches are the complement of the inequality match set

                                if (cond_group_index not in eq_ne_index):
                                    self._logger.debug("Inequality match conditions require further indexing for condition group {0}".format(cond_group_index))

                                    eq_ne_index[cond_group_index] = {}
                                    index_ne(ne_matches, eq_ne_index[cond_group_index])

                                self._logger.debug("NE matches: {0} on {1}, EQ matches: <none>".format(ne_matches, filter_keys_ne))

                                for possible_match_key, possible_match_array in self._results[-2][cond_group_index][F.OP_NE].iteritems():
                                    for match_value in possible_match_array:
                                        if (id(match_value) not in eq_ne_index[cond_group_index]):
                                            match_callback(instr, result, match_value)
                                        else:
                                            break

                            elif ((len(filter_keys_eq) > 0) and (len(filter_keys_ne) == 0)):
                                # If there are only equality conditions, the desired matches are simply all the equality matches
                                # Iteration over all matches is only required if there's a match callback
                                self._logger.debug("All field relationship condition(s) (EQ) in group {0} hold: preserving hit".format(cond_group_index))
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
        """Applies appropriate logic to a data source response based on the query step and success of the data source request.
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
        """Delete's the previous query step's results. Important for multi-step queries that yield large result sets.
        """
        del self._results[0]

    def _join_match_callback(self, instr, subject, match):
        """The callback invoked when a right-hand result is determined to be eligible for joining to a left-hand result.

           subject is the right-hand result, match is the left-hand result.

           If the join step has been specified to support multiple results, the joined data will be a list of n joined objects. Otherwise, the joined data will be the first single right-hand object found to be eligible for the join.
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
        """Request data from the data source associated with the instruction and process the response.
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
        """Execute the query. Runs each query step from left to right.

           If scroll is True, the data source's Elasticsearch-style scroll API will be used to retrieve results.
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

