package simpledb;

import java.util.ArrayList;
import java.util.List;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int agfield;
    private Op what;
    private List<Tuple> result;
    private TupleDesc tupleDesc;
    private List<Integer> count;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
                result=new ArrayList<>();
                count=new ArrayList<>();
                if (gbfield==NO_GROUPING){
                    Type typearr[]=new Type[1];
                    typearr[0]=Type.INT_TYPE;
                    tupleDesc=new TupleDesc(typearr);
                    Tuple tuple=new Tuple(tupleDesc);
                    IntField intField=new IntField(0);
                    tuple.setField(0,intField);
                    result.add(tuple);
                    count.add(0);
                }
                else {
                    Type typearr[] =new Type[2];
                    typearr[0]=gbfieldtype;
                    typearr[1]= Type.INT_TYPE;
                    tupleDesc=new TupleDesc(typearr);
                }
                this.gbfield=gbfield;
                this.agfield=afield;
                this.what=what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (gbfield == NO_GROUPING) {
            count.set(0, count.get(0) + 1);
            IntField newField = new IntField(count.get(0));
            result.get(0).setField(0, newField);
        } else {
            Field tupGbfield;
            if (tup.getField(gbfield).getType() == Type.INT_TYPE) {
                tupGbfield = (IntField) tup.getField(gbfield);
            } else {
                tupGbfield = (StringField) tup.getField(gbfield);
            }
            boolean flag=false;
            for (int i = 0; i < result.size(); i++) {
                if (result.get(i).getField(0).equals(tupGbfield)) {
                   count.set(i,count.get(i)+1);
                   IntField newField=new IntField(count.get(i));
                   result.get(i).setField(1,newField);
                   flag=true;
                   break;
                }
            }
            if(!flag){
                IntField newField=new IntField(1);
                Tuple newTuple = new Tuple(tupleDesc);
                newTuple.setField(0, tupGbfield);
                newTuple.setField(1, newField);
                result.add(newTuple);
                count.add(1);
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        Iterable iterable = result;
        TupleIterator tupleIterator = new TupleIterator(tupleDesc, iterable);
        return tupleIterator;
    }

}
