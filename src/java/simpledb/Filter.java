package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    private Predicate predicate;
    private OpIterator iterator;
    private OpIterator[] opIterators=null;
    private boolean open=false;
    Tuple next=null;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // some code goes here
        predicate=p;
        iterator=child;

    }

    public Predicate getPredicate() {
        // some code goes here
        return predicate;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return iterator.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        iterator.open();
        open=true;
    }

    public void close() {
        // some code goes here
        iterator.close();
        open=false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here

        iterator.rewind();
    }
  public boolean hasNext() throws DbException, TransactionAbortedException {
     if (open) {
         if (next==null)
            next=fetchNext();                                                           //fetchnext()会将接代器后移，注意next和hasnext要保持迭代器当前位置
         return next!=null;
        }
        throw new IllegalStateException();
    }
   public Tuple next() throws DbException, TransactionAbortedException,NoSuchElementException {
        if (next==null)
            next=fetchNext();
        if (next==null){
            throw new NoSuchElementException();
        }
        Tuple res=next;
        next=null;
        return res;
    }
    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        while (iterator.hasNext()){
            Tuple temp=iterator.next();
            if (predicate.filter(temp)){
                return temp;
            }
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        if (opIterators==null) {
            opIterators = new OpIterator[1];
            opIterators[0]=iterator;
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
            opIterators[children.length]=iterator;
    }

}
