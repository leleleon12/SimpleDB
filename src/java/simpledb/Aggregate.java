package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private int afield;
    private int gfield;
    private OpIterator tupleIterator;
    private Aggregator.Op op;
    private Type afieldType;
    private Type gfieldType;
    private Aggregator aggregator;
    private OpIterator opIterator;
    private OpIterator[] opIterators=null;
   // private Tuple next=null;
    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
	// some code goes here
        tupleIterator=child;
        this.afield=afield;
        this.gfield=gfield;
        op=aop;
        this.afieldType=child.getTupleDesc().getFieldType(afield);
        if (gfield!=-1){
            gfieldType=child.getTupleDesc().getFieldType(gfield);
        }
        else {
            gfieldType=null;
        }
        next=null;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
        if (gfield==-1){
            return Aggregator.NO_GROUPING;
        }
        else return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
        if (gfield==-1){
            return null;
        }
        else return tupleIterator.getTupleDesc().getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
	return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
	return tupleIterator.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
	return op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	// some code goes here
        open=true;
        tupleIterator.open();
        if (afieldType==Type.INT_TYPE){
            aggregator=new IntegerAggregator(groupField(),gfieldType,afield,op);
            while (tupleIterator.hasNext()) {
                aggregator.mergeTupleIntoGroup(tupleIterator.next());
            }
            opIterator = aggregator.iterator();
            opIterator.open();
        }
        else {
             aggregator=new StringAggregator(groupField(),gfieldType,afield,op);
            while (tupleIterator.hasNext()) {
                aggregator.mergeTupleIntoGroup(tupleIterator.next());
            }
            opIterator = aggregator.iterator();
            opIterator.open();
        }
        tupleIterator.close();
    }
//    public boolean hasNext() throws TransactionAbortedException, DbException {
//        if (next==null){
//            next=fetchNext();
//        }
//        return next!=null;
//    }
//    public Tuple next() throws TransactionAbortedException, DbException {
//        if (next==null)
//            next=fetchNext();
//        if (next==null){
//            throw new NoSuchElementException();
//        }
//        Tuple res=next;
//        next=null;
//        return res;
//    }
    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
        if (opIterator.hasNext()){
            return opIterator.next();
        }
        else return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	// some code goes here
        close();
        open();
        opIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
	return tupleIterator.getTupleDesc();
    }

    public void close() {
	// some code goes here
        opIterator.close();
        open=false;
    }

    @Override
    public OpIterator[] getChildren() {
	// some code goes here
        if (opIterators==null) {
            opIterators = new OpIterator[1];
            opIterators[0]=tupleIterator;
        }
        return opIterators;
    }

    @Override
    public void setChildren(OpIterator[] children) {
	// some code goes here
        opIterators=new OpIterator[children.length+1];
        for (int i = 0; i <children.length ; i++) {
            opIterators[i]=children[i];
        }
        opIterators[children.length]=tupleIterator;
    }
    
}
