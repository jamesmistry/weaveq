import random
import json

class TestRecord(object):
    def __init__(self, collection, num_keys = 1):

        if (collection is None):
            raise ValueError("collection must be != None")

        if (num_keys < 1):
            raise ValueError("num_keys must be >= 1")

        self.rec = {}
        self.keys = []

        for key_index in xrange(num_keys):
            this_key = "keydata_{0}".format(key_index)
            this_key_name = "key_{0}_{1}".format(collection.index, key_index)
            self.rec[this_key_name] = this_key
            self.keys.append((this_key_name, this_key,))

        self.rec["field_0"] = "data_{0}_0".format(collection.index)
        self.rec["field_1"] = "data_{0}_1".format(collection.index)
        self.rec["doctype"] = "jiggleqsystest_set{0}".format(collection.index)
       
class TestCollection(object):
    def __init__(self, index, size):
        
        if (index < 0):
            raise ValueError("index must be >= 0")

        if (size <= 0):
            raise ValueError("size must be > 0")

        self.index = index
        self.recs = []

        for record_index in xrange(size):
            self.recs.append(TestRecord(self, (record_index % 3) + 1).rec)

        random.shuffle(self.recs)

