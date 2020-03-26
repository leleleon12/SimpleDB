package simpledb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static simpledb.Predicate.Op.GREATER_THAN;
import static simpledb.Predicate.Op.LESS_THAN;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

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
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        result = new ArrayList<>();
        count = new ArrayList<>();
        if (gbfield == NO_GROUPING) {
            Type typearr[] = new Type[1];
            typearr[0] = Type.INT_TYPE;
            tupleDesc = new TupleDesc(typearr);
            Tuple tuple = new Tuple(tupleDesc);
            IntField intField = new IntField(0);
            tuple.setField(0, intField);
            result.add(tuple);
            count.add(0);
        } else {
            Type typearr[] = new Type[2];
            typearr[0] = gbfieldtype;
            typearr[1] = Type.INT_TYPE;
            tupleDesc = new TupleDesc(typearr);
        }
        this.gbfield = gbfield;
        this.agfield = afield;
        this.what = what;

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here

        if (gbfield == NO_GROUPING) {
            IntField resAgField = (IntField) (result.get(0).getField(0));
            IntField tupAgField = (IntField) (tup.getField(agfield));
            switch (what) {
                case AVG: {
                    int newRes;
                    if (count.get(0) != 0) {
                        count.set(0, count.get(0) + 1);
                        newRes = (resAgField.getValue() * (count.get(0) - 1) + tupAgField.getValue()) / count.get(0);
                    } else {
                        newRes = tupAgField.getValue();
                    }
                    IntField newField = new IntField(newRes);
                    result.get(0).setField(0, newField);
                    count.set(0, count.get(0) + 1);
                    break;
                }
                case COUNT: {

                    count.set(0, count.get(0) + 1);
                    IntField newField = new IntField(count.get(0));
                    result.get(0).setField(0, newField);
                    break;
                }
                case MAX: {
                    if ((resAgField.getValue() < tupAgField.getValue()) || (count.get(0) == 0)) {
                        result.get(0).setField(0, tupAgField);
                    }
                    count.set(0, count.get(0) + 1);
                    ;
                    break;
                }
                case MIN: {
                    if ((resAgField.getValue() > tupAgField.getValue()) || (count.get(0) == 0)) {
                        result.get(0).setField(0, tupAgField);
                    }
                    count.set(0, count.get(0) + 1);
                    break;
                }
                case SUM: {
                    IntField newField = new IntField(resAgField.getValue() + tupAgField.getValue());
                    result.get(0).setField(0, newField);
                    break;
                }
            }
        } else {
            Field tupGbfield;
            if (tup.getField(gbfield).getType() == Type.INT_TYPE) {
                tupGbfield = (IntField) tup.getField(gbfield);
            } else {
                tupGbfield = (StringField) tup.getField(gbfield);
            }
            IntField tupAgField = (IntField) (tup.getField(agfield));
            Tuple newTuple = new Tuple(tupleDesc);
            newTuple.setField(0, tupGbfield);
            newTuple.setField(1, tupAgField);
            if (result.isEmpty()) {
                if (what == Op.COUNT) {
                    IntField newField = new IntField(1);
                    newTuple.setField(1, newField);
                }
                result.add(newTuple);
                count.add(1);
                return;
            }
            boolean flag = false;
            switch (what) {
                case SUM: {
                    for (int i = 0; i < result.size(); i++) {
                        if (result.get(i).getField(0).equals(tupGbfield)) {
                            IntField resAgfield = (IntField) result.get(i).getField(1);
                            int newRes = resAgfield.getValue() + tupAgField.getValue();
                            IntField newField = new IntField(newRes);
                            result.get(i).setField(1, newField);
                            flag = true;
                            break;
                        }
                    }
                    if (!flag) {
                        result.add(newTuple);
                    }
                    break;
                }
                case MIN: {
                    for (int i = 0; i < result.size(); i++) {
                        if (result.get(i).getField(0).equals(tupGbfield)) {
                            if (result.get(i).getField(1).compare(GREATER_THAN, tupAgField)) {
                                result.get(i).setField(1, tupAgField);
                                flag = true;
                                break;
                            }
                        }
                    }
                    if (!flag) {
                        result.add(newTuple);
                    }
                    break;
                }
                case MAX: {
                    for (int i = 0; i < result.size(); i++) {
                        if (result.get(i).getField(0).equals(tupGbfield)) {
                            if (result.get(i).getField(1).compare(LESS_THAN, tupAgField)) {
                                result.get(i).setField(1, tupAgField);
                                flag = true;
                                break;
                            }
                        }
                    }
                    if (!flag) {
                        result.add(newTuple);
                    }
                    break;
                }
                case AVG: {
                    for (int i = 0; i < result.size(); i++) {
                        if (result.get(i).getField(0).equals(tupGbfield)) {
                            IntField resAgfield = (IntField) result.get(i).getField(1);
                            count.set(i, count.get(i) + 1);
                            int newRes = (resAgfield.getValue() * (count.get(i) - 1) + tupAgField.getValue()) / count.get(i);
                            IntField newField = new IntField(newRes);
                            result.get(i).setField(1, newField);
                            flag = true;
                            break;
                        }
                    }
                    if (!flag) {
                        result.add(newTuple);
                        count.add(1);
                    }
                    break;
                }
                case COUNT: {
                    for (int i = 0; i < result.size(); i++) {
                        if (result.get(i).getField(0).equals(tupGbfield)) {
                            count.set(i, count.get(i) + 1);
                            IntField newField = new IntField(count.get(i));
                            result.get(i).setField(1, newField);
                            flag = true;
                            break;
                        }
                    }
                    if (!flag) {
                        IntField newField = new IntField(1);
                        newTuple.setField(1, newField);
                        result.add(newTuple);
                    }
                    count.add(1);
                    break;
                }
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        Iterable iterable = result;
        TupleIterator tupleIterator = new TupleIterator(tupleDesc, iterable);
        return tupleIterator;
    }
}
