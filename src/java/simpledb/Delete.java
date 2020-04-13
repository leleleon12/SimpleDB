package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private OpIterator childOpiter;
    //private Tuple next=null;
    private OpIterator[] opIterators;
    private boolean hasDeleted=false;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        tid=t;
        childOpiter=child;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return childOpiter.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        childOpiter.open();
        open=true;
    }

    public void close() {
        // some code goes here
        childOpiter.close();
        open=false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        childOpiter.rewind();
    }
//    public Tuple next() throws TransactionAbortedException, DbException {
//        if (next==null){
//            next=fetchNext();
//        }
//        Tuple tuple=next;
//        next=null;
//        return tuple;
//    }
//    public boolean hasNext() throws TransactionAbortedException, DbException {
//        if (next==null){
//            next=fetchNext();
//        }
//        if (next==null){
//            return false;
//        }
//        else return true;
//    }
    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (!hasDeleted) {
            int i = 0;
            while (childOpiter.hasNext()) {
                try {
                    Database.getBufferPool().deleteTuple(tid, childOpiter.next());
                } catch (IOException e) {
                    e.printStackTrace();
                }
                i++;
            }
            IntField intField = new IntField(i);
            Type[] types = {Type.INT_TYPE};
            String[] strings = {"nums"};
            TupleDesc tupleDesc = new TupleDesc(types, strings);
            Tuple tuple = new Tuple(tupleDesc);
            tuple.setField(0, intField);
            hasDeleted=true;
            return tuple;
        }
        else return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        if (opIterators==null) {
            opIterators = new OpIterator[1];
            opIterators[0]=childOpiter;
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
        opIterators[children.length]=childOpiter;
    }

}
