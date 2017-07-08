"""!
@package jiggleq.relations Classes for expressing the relationships between fields that override Python operators to provide a simple API and for ease of parsing.
"""

import copy

import jqexception

class FieldMustBeRelated(jqexception.JiggleQError):
    """!
    Thrown when only a field object is provided when an expression of the form [field] [operator] [field] is expected
    """
    def __init__(self):
        """!
        Constructor.
        """
        super(FieldMustBeRelated, self).__init__("Each left-hand field specified must be related to a right-hand field by a comparison operator. For example, F(leftField) == F(rightField) instead of F(leftField)")

class FieldRelationshipNotInParentheses(jqexception.JiggleQError):
    """!
    Thrown when there's ambiguity caused by a field relationship not being enclosed in parentheses

    @param leftField F: Left-hand field in the expression that caused the exception
    @param op int: Value identifying the operator type in the expression that caused the exception
    @param rightField F: Right-hand field in the expression that caused the exception
    """
    def __init__(self, leftField, op, rightField):
        """!
        Constructor.
        """
        super(FieldRelationshipNotInParentheses, self).__init__("You didn't put a field relationship mapping in parentheses: write (F({0}) {1} F({2})) instead of F({0}) {1} F({2})".format(leftField, op, rightField))

class DefaultFieldProxy(object):
    """!
    A field value proxy class that simply represents the original value unmodified

    @param name string: The name of the field to proxy
    @param value object: The original value of the field to proxy

    @return The original field value
    """
    def __call__(self, name, value):
        """!
        Entry point for the proxy. Called when the query engine needs to find out the correct representation of a value in a query step result.

        This class represents the default behaviour, which is simply to return the original value unmodified to the caller.

        @param name string: The name of the field from which the value to proxy has come
        @param value: The value to be proxied

        @return The proxied representation of the supplied value
        """
        return value

class F(object):
    """!
    @brief Represents a field relationship expression

    Overrides a set of operators that can be used to express the desired relationship between two F objects and a set of operators that can be used to AND or OR two or more F object relationships together. Internally, it represents these relationships as a tree, each branch of which contains a set of nodes representing field relationships AND'ed together.
    """

    ## Constant defining an equality relationship between fields
    OP_EQ = 0

    ## Constant defining an inequality relationship between fields
    OP_NE = 1

    @staticmethod
    def op_str(opcode):
        """!
        Converts a comparison opcode to a string.

        @param opcode int: The opcode to be represented as a string

        @return the string representation of the supplied opcode
        """
        if (opcode == F.OP_EQ):
            return "=="
        elif (opcode == F.OP_NE):
            return "!="

    def __init__(self, name, proxy = None):
        """!
        Constructor.
        
        If no field proxy is specified, a default transparent one is used.

        @param name String: name of the field
        @param proxy object: object to use as the field value proxy. It must expose a method with the following signature: \_\_call\_\_(self, name, value)
        """

        ## @var name
        # Field's name
        self.name = name

        ## @var tree
        # Tree object for representing the relationship with other nodes
        self.tree = None

        ## @var proxy
        # Proxy field object for use with this field
        self.proxy = None

        if (proxy is None):
            self.proxy = DefaultFieldProxy()
        else:
            self.proxy = proxy

        ## @var op
        # Opcode representing a comparison operation (set using a comparison operator)
        self.op = None

        ## @var rhs
        # Right-hand side field to relate to this field (set using a comparison operator)
        self.rhs = None

    def __eq__(self, rhs):
        """!
        Creates a node to represent the expression: self == rhs

        @param rhs F: right-hand side field relationship object with which to express an equality relationship

        @return An @c F representing the resulting expression
        """
        if (rhs.tree is not None):
            # Operator precedence means that individual expressions within compound expressions need to be enclosed in parentheses - raising here will hopefully make the problem clear
            raise FieldRelationshipNotInParentheses(self.name, F.op_str(OP_EQ), rhs.name)
        self.op = F.OP_EQ
        self.rhs = rhs
        self.tree = ConditionNode(None, self.name, F.OP_EQ, rhs.name, self.proxy, rhs.proxy, "{0} {1} {2}".format(self.name, F.op_str(self.op), rhs.name))
        return self

    def __ne__(self, rhs):
        """!
        Creates a node to represent the expression: self != rhs

        @param rhs F: right-hand side field relationship object with which to express an inequality relationship

        @return An @c F representing the resulting expression
        """
        if (rhs.tree is not None):
            # Operator precedence means that individual expressions within compound expressions need to be enclosed in parentheses - raising here will hopefully make the problem clear
            raise FieldRelationshipNotInParentheses(self.name, F.op_str(OP_NE), rhs.name)
        self.op = F.OP_NE
        self.rhs = rhs
        self.tree = ConditionNode(None, self.name, F.OP_NE, rhs.name, self.proxy, rhs.proxy, "{0} {1} {2}".format(self.name, F.op_str(self.op), rhs.name))
        return self

    def __and__(self, rhs):
        """!
        AND the left-hand expression to the right-hand expression by appending a copy of the LHS tree to all leaf nodes of the RHS tree, creating a tree representing: (self) and (rhs)

        @param rhs F: right-hand side field relationship object with which to express a conjoined relationship

        @return An @c F representing the resulting expression
        """
        leaves = rhs.tree.leaves()

        for leaf in leaves: leaf.add_child(copy.deepcopy(self.tree))
        self.tree = rhs.tree

        return self

    def __or__(self, rhs):
        """!
        OR the left-hand expression to the right-hand expression by creating a tree whose root node is the parent of the LHS and RHS condition trees, creating a tree representing: (self) or (rhs)

        @param rhs F: right-hand side field relationship object with which to express a disjoined relationship

        @return An @c F representing the resulting expression
        """
        r = ConditionNode(None)
        r.add_child(self.tree)
        r.add_child(rhs.tree)
        self.tree = r
        return self

class ConditionNode(object):
    """!
    A node in a condition tree.

    A tree node containing an LHS field name/proxy, operator and RHS field name/proxy. Used at query-time to evaluate result sets and at build-time to walk a condition tree.
    """
    def __init__(self, parent, left_field = None, op = None, right_field = None, lhs_proxy = None, rhs_proxy = None, label = "?"):
        """!
        Constructor.

        @param parent ConditionNode: parent node to which the new object should be associated
        @param left_field F: Left-hand side field object
        @param op int: Value denoting relationship operator
        @param right_field F: Right-hand side field object
        @param lhs_proxy object: An object to use for proxying the left_field value
        @param rhs_proxy object: An object to use for proxying the right_field value
        @param label string: A label to associate with the condition represented by the node

        @see F
        """

        ## @var parent
        # ConditionNode that is the parent of this node in the tree
        self.parent = None

        ## @var left_field
        # Field object that represents the left-hand side of the condition
        self.left_field = left_field

        ## @var op
        # Comparison operator code
        self.op = op

        ## @var right_field
        # Field object that represents the right-hand side of the condition
        self.right_field = right_field

        ## @var _edges
        # Outgoing edges from this node as an array of other ConditionNode objects
        self._edges = []

        ## @var label
        # Node's string label for help in debugging
        self.label = label

        ## @var lhs_proxy
        # Proxy object for use with left-hand side field values
        self.lhs_proxy = lhs_proxy

        ## @var rhs_proxy
        # Proxy object for use with right-hand side field values
        self.rhs_proxy = rhs_proxy

        if (parent is not None):
            parent.add_child(self)

    def __repr__(self):
        """!
        Produce a string representation of the field relationship in the form [lhs] [operator] [rhs].

        @return The string representation
        """
        if ((self.op is None) and (self.left_field is None) and (self.right_field is None)):
            return "*"
        else:
            return "{0} {1} {2}".format(self.left_field, F.op_str(self.op), self.right_field)

    def add_child(self, child):
        """!
        Add a child node.

        @param child ConditionNode: The child to associate with the node.

        @return The child added
        """
        self._edges.insert(0, child)
        self._edges[0].parent = self
        return self._edges[0]

    def leaf(self):
        """!
        Is this node a leaf node - does it have exactly 0 children?

        @return @c True if the node is a leaf node, @c False if it isn't
        """
        return (len(self._edges) == 0)

    def leaves(self):
        """!
        Get a list of leaf nodes in the sub-tree represented by this node.

        @return list of leaf nodes
        """
        leaves = []
        for node in self.walk():
            if (node.leaf()):
                leaves.append(node)

        return leaves

    def walk_back(self):
        """!
        From this node, walk backwards to the root node of the tree.

        Performs iteration using @c yield
        """
        to_follow = self
        yield to_follow

        while to_follow.parent is not None:
            to_follow = to_follow.parent
            yield to_follow
    
    def walk(self):
        """!
        Perform a pre-order depth-first walk of the sub-tree represented by this node.

        Performs iteration using @c yield
        """
        to_follow = self
        yield to_follow

        if (len(self._edges) == 0):
            return

        edge_stack = [[]]
        for edge in to_follow._edges: edge_stack[0].append(edge)
        
        while (True):

            if (len(edge_stack[-1]) == 0):
                edge_stack.pop()

            if (len(edge_stack) > 0):
                origin_edge = edge_stack[-1][-1]
                to_follow = origin_edge

                edge_stack[-1].pop()

                if (len(edge_stack[-1]) == 0):
                    edge_stack.pop()

                edge_stack.append([])
                for edge in to_follow._edges: edge_stack[-1].append(edge)

                yield to_follow

            if (len(edge_stack) == 0):
                break

class TargetConditions(object):
    """!
    Simplifies a condition tree into a useful helper for evaluating result sets against condition logic.
    
    Maintains a list of ANDed conditions ("conjunctions") and the right-hand-side field names on which they, as a whole, depend. This allows evaluation to be short-circuited if a field that is listed as a dependency doesn't exist.
    """

    def __str__(self):
        return str(self.conjunctions)

    def __init__(self, tree):
        """!
        Constructor.

        @param tree ConditionNode: The tree from which the helper object should be created
        """

        ## @var tree
        # The source tree from which the object is created
        self.tree = tree

        ## @var conjunctions
        # ANDed conditions grouped together as lists
        self.conjunctions = []

        ## @var rhs_dependencies
        # List of right-hand side field names that must exist in order for the conditions to be satisfied
        self.rhs_dependencies = []

        field_counts = {}

        if (self.tree is None):
            raise FieldMustBeRelated()

        leaves = self.tree.leaves()
        for leaf in leaves:
            fields_encountered = {}
            current_conj = []
            for node in leaf.walk_back():
                if (node.right_field is None):
                    continue

                if (node.right_field not in fields_encountered):
                    if (node.right_field in field_counts):
                        field_counts[node.right_field] += 1
                    else:
                        field_counts[node.right_field] = 1
                    fields_encountered[node.right_field] = None

                current_conj.append(node)

            self.conjunctions.append(current_conj)

            for field, count in field_counts.iteritems():
                if (count >= len(leaves)):
                    self.rhs_dependencies.append(field)


