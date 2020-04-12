package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private int tableId;
    private OpIterator childOper;
    private TransactionId tid;
    private boolean open=false;
    private boolean hasCalled=false;
    private OpIterator[] opIterators;
    private Tuple next=null;
    private Tuple res;
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        tid=t;
        childOper=child;
        this.tableId=tableId;
        Type type[] = {Type.INT_TYPE};
        String field[] = {"nums"};
        TupleDesc tupleDesc = new TupleDesc(type, field);
        res = new Tuple(tupleDesc);
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return res.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        childOper.open();
        open=true;
    }

    public void close() {
        // some code goes here
        childOper.close();
        open=false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        childOper.rewind();
        open=true;
    }
    public boolean hasNext() throws TransactionAbortedException, DbException {
        if (next==null){
            next=fetchNext();
        }
        if (next==null){
            return false;
        }
        else return true;
    }
    public Tuple next() throws TransactionAbortedException, DbException {
        if (next==null){
            next=fetchNext();
        }
        Tuple tuple=next;
        next=null;
        return tuple;
    }
    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (!hasCalled) {
            int i = 0;
            while (childOper.hasNext()) {
                try {
                    Database.getBufferPool().insertTuple(tid, tableId, childOper.next());
                    i++;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            IntField intField = new IntField(i);
            res.setField(0, intField);
            hasCalled=true;
            return res;
        }
        else return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        if (opIterators==null) {
            opIterators = new OpIterator[1];
            opIterators[0]=childOper;
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
        opIterators[children.length]=childOper;
    }
}
