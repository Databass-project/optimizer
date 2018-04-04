# Additional classes implemented by the group

## Optimizer

#### `DPOptimizer`

This class implements the dynamic programming optimizer to compute the operator tree with minimum cost. This class only examines left-deep trees without cartesian products (joins without common attributes are not considered. For example, if we have (A join B join C), it will NOT consider (A join C) since A and C do not have a common join attribute.

The process through which the DP Optimizer computes the optimal plan is as follows:
1. It selects the best plan for a single-relation. In our case, we push down selections as early as possible before joins. This is done in `computeSingleRelationPlan` method.
2. It computes the best plan for two-relation joins. This is done by `computeBaseJoinRelationPlan` method. In this method, we flip the left and right table of each join should this yield lower cost of join.
3. Then for each subtree, the code iterates through the join condition list and grows the subtree by appending join operators on top. Since we only consider left-deep trees, we look through the remaining join conditions one of whose attribute is in the schema of the root of the subtree. At ith iteration, we keep the best plan for joins with i conditions.
4. We do this until we have a single tree which contains all the join conditions. By DP property, this is the optimal plan for implementing the entire join query.

##### `OperatorUtils`
- this class imitates the features of `RandomInitialPlan` class used by `RandomOptimizer`. It provides methods to initialize the single-relation operators such as `Scan` and `Select`, as well as `Project`.

## Join operators

#### `BlockNestedJoin`
- This class implements block-nested join which uses B buffers, where 1 buffer is allocated for accumulating join output tuples, 1 buffer for scanning the right table and (B-2) buffers to load tuples from the left table.
- Rather than simulating B-2 buffers by creating a list/collection of `Batch` object, we simply make one `Batch` object whose capacity = (B-2)*batchsize and mostly reuse the code given in `NestedJoin` class.

## OrderBy

### OrderBy operator
This class implements the OrderBy operator which order the resultings tuples given a list of attributes. The hard work is done in the open() method which needs to sort all the tuples of the base operator. Then the next() method only needs to retrieve the sorted tuples one batch at a time. Since the sorting operation obviously involves a materialization of the base operator, it is not done in parallel to any other operation. In particular, since the orderby operation is not done in parallel with any join operation, it can use every available buffer to perform the (external) sorting opeation.

### Scaner and parser
The ORDERBY operation was not supported by either the scaner or parser. We modified both of those to support ORDERBY in queries. An example query with an ORDERBY operation would look something like :

SELECT *
FROM tablename
ORDERBY tablename.attributeA, tablename.attributeB

Which would return the table sorted by attributeA and then by attributeB for tuple with identical attributeA.




