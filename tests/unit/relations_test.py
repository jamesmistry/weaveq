"""@package relations_test
Tests for weaveq.relations
"""

import unittest

from weaveq.relations import F
from weaveq.relations import TargetConditions
from weaveq.relations import FieldRelationshipNotInParentheses
from weaveq.relations import ConditionNode

class WalkTracer(object):
    """A helper class for tracing the ConditionNode.walk() behaviour.
    """
    def __init__(self, target):
        """Initialise the object with a trace of a walk over the target tree."""
        self.trace = ""

        for node in target.walk():
            # Make sure a node's parent thinks it's a child
            if (node.parent is not None):
                confusedChild = True
                for child in node.parent._edges:
                    if (child is node):
                        confusedChild = False
                        break
                if (confusedChild):
                    raise Exception("Child refers to parent, but parent doesn't refer to child")
            if (len(self.trace) == 0):
                self.trace = "{0}".format(str(node))
            else:
                self.trace = "{0},{1}".format(self.trace, str(node))

class TestConditionNode(unittest.TestCase):
    """Tests ConditionNode class
    """
    def test_walk(self):
        """Ensure that a walk discovers all nodes that were added strictly in insertion order. This allows a walk to determine condition evaluation order."""
        subject = ConditionNode(None)
        subject.add_child(ConditionNode(None, "f1", F.OP_EQ, "f2", "n1")).add_child(ConditionNode(None, "f3", F.OP_EQ, "f4", "n2"))
        branch = subject.add_child(ConditionNode(None, "f5", F.OP_NE, "f6", "n3"))
        branch.add_child(ConditionNode(None, "f7", F.OP_NE, "f8", "n4"))
        branch.add_child(ConditionNode(None, "f9", F.OP_NE, "f10", "n5"))

        tracer = WalkTracer(subject)
        self.assertEqual(tracer.trace, "*,f1 == f2,f3 == f4,f5 != f6,f7 != f8,f9 != f10")

    def test_walk_singlenode(self):
        """Ensure a walk works even if there's just a single node in the tree"""
        subject = ConditionNode(None)
        tracer = WalkTracer(subject)
        self.assertEqual(tracer.trace, "*")

    def test_walkback(self):
        """Ensure a backwards walk discovers every node from the target to the root. TargetConditions already exercises this method, as well as the leaf-related ones."""
        subject = ConditionNode(None)
        leaf = subject.add_child(ConditionNode(None, "f1", F.OP_EQ, "f2", "n1")).add_child(ConditionNode(None, "f3", F.OP_EQ, "f4", "n2")).add_child(ConditionNode(None, "f5", F.OP_NE, "f6", "n3"))
        conds = TargetConditions(leaf)
        self.assertEqual(str(conds.conjunctions), "[[f5 != f6, f3 == f4, f1 == f2]]")

class TestF(unittest.TestCase):
    """Tests F class
    """
    def test_opcode_str(self):
        self.assertEqual(F.op_str(F.OP_EQ), "==")
        self.assertEqual(F.op_str(F.OP_NE), "!=")

    def test_create(self):
        field = F("test")
        self.assertEqual(field.name, "test")

    def test_relationship_equal(self):
        field1 = F("left")
        field2 = F("right")
        expr = (field1 == field2)
        
        self.assertEqual(expr.op, F.OP_EQ)
        conds = TargetConditions(expr.tree)

        self.assertEqual(str(conds.conjunctions), "[[left == right]]")

    def test_relationship_notequal(self):
        field1 = F("left")
        field2 = F("right")
        expr = (field1 != field2)

        self.assertEqual(expr.op, F.OP_NE)
        conds = TargetConditions(expr.tree)

        self.assertEqual(str(conds.conjunctions), "[[left != right]]")

    def test_relationship_equal_noparens(self):
        field1 = F("left")
        field2 = F("right")
        
        self.assertRaises(FieldRelationshipNotInParentheses, expr = field1 == field2)

    def test_relationship_notequal_noparens(self):
        field1 = F("left")
        field2 = F("right")

        self.assertRaises(FieldRelationshipNotInParentheses, expr = field1 != field2)

    def test_relationship_and(self):
        expr1 = (F("f1") == F("f2"))
        expr2 = (F("f3") != F("f4"))
        expr3 = (expr1 & expr2)

        conds = TargetConditions(expr3.tree)

        self.assertEqual(str(conds.conjunctions), "[[f1 == f2, f3 != f4]]")

    def test_relationship_or(self):
        expr1 = (F("f1") == F("f2"))
        expr2 = (F("f3") != F("f4"))
        expr3 = (expr1 | expr2)

        conds = TargetConditions(expr3.tree)

        self.assertEqual(str(conds.conjunctions), "[[f1 == f2], [f3 != f4]]")

    def test_relationship_grouped(self):
        expr1 = (F("f1") == F("f2"))
        expr2 = (F("f3") == F("f4"))
        expr3 = (F("f5") == F("f6"))
        expr4 = (expr1 & (expr2 | expr3))

        conds = TargetConditions(expr4.tree)

        self.assertEqual(str(conds.conjunctions), "[[f1 == f2, f3 == f4], [f1 == f2, f5 == f6]]")

class TestTargetConditions(unittest.TestCase):
    def test_conjuncts(self):
        subject = ConditionNode(None)
        subject.add_child(ConditionNode(None, "f1", F.OP_EQ, "f2", "n1")).add_child(ConditionNode(None, "f3", F.OP_EQ, "f4", "n2"))
        branch = subject.add_child(ConditionNode(None, "f5", F.OP_NE, "f6", "n3"))
        branch.add_child(ConditionNode(None, "f7", F.OP_NE, "f8", "n4"))
        branch.add_child(ConditionNode(None, "f9", F.OP_NE, "f10", "n5"))

        conds = TargetConditions(subject)
        self.assertEqual(str(conds.conjunctions), "[[f3 == f4, f1 == f2], [f7 != f8, f5 != f6], [f9 != f10, f5 != f6]]")

    def test_rhs_deps(self):
        subject = ConditionNode(None)
        subject.add_child(ConditionNode(None, "f1", F.OP_EQ, "f2", "n1")).add_child(ConditionNode(None, "f3", F.OP_EQ, "f4", "n2"))
        branch = subject.add_child(ConditionNode(None, "f5", F.OP_NE, "f2", "n3"))
        branch.add_child(ConditionNode(None, "f6", F.OP_NE, "f4", "n4"))
        branch.add_child(ConditionNode(None, "f7", F.OP_NE, "f4", "n5"))

        conds = TargetConditions(subject)
        conds.rhs_dependencies.sort()
        expected = ["f2", "f4"]
        expected.sort()
        self.assertEqual(conds.rhs_dependencies, expected)
