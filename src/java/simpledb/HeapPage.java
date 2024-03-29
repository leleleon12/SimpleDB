package simpledb;

import java.lang.reflect.Array;
import java.util.*;
import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {

    final HeapPageId pid;
    final TupleDesc td;
    final byte header[];
    private Tuple tuples[];
    final int numSlots;
    private TransactionId dirtierTid;
    private boolean dirty;

    byte[] oldData;
    private final Byte oldDataLock=new Byte((byte)0);

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i=0; i<header.length; i++)
            header[i] = dis.readByte();
        
        tuples = new Tuple[numSlots];
        try{
            // allocate and read the actual records of this page
            for (int i=0; i<tuples.length; i++)
                if (isSlotUsed(i)) {
                    tuples[i] = readNextTuple(dis, i);
                }
                else tuples[i]=null;
        }catch(NoSuchElementException e){
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /** Retrieve the number of tuples on this page.
        @return the number of tuples on this page
    */
    private int getNumTuples() {        
        // some code goes here
        return Math.floorDiv(BufferPool.getPageSize()*8,td.getSize()*8+1);
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {        
        
        // some code goes here
        if(getNumTuples()%8==0){
            return getNumTuples()/8;
        }
        else
        return (int) Math.ceil(getNumTuples()/8+1);
                 
    }
    
    /** Return a view of this page before it was modified
        -- used by recovery */
    public HeapPage getBeforeImage(){
        try {
            byte[] oldDataRef = null;
            synchronized(oldDataLock)
            {
                oldDataRef = oldData;
            }
            return new HeapPage(pid,oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }
    
    public void setBeforeImage() {
        synchronized(oldDataLock)
        {
        oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
    // some code goes here
        return pid;
    //throw new UnsupportedOperationException("implement this");
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i=0; i<td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (int i=0; i<header.length; i++) {
            try {
                dos.writeByte(header[i]);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i=0; i<tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j=0; j<td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should be updated to reflect
     *   that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        if(!t.getRecordId().getPageId().equals(pid)){
            throw new DbException("tuple is not in this page");
        }
        if (t.getRecordId().getTupleNumber()+1>numSlots){
            throw new DbException("tpno exceed the page");
        }
        if (!isSlotUsed(t.getRecordId().getTupleNumber())){
            throw new DbException("slot is already empty");
        }
        tuples[t.getRecordId().getTupleNumber()]=null;
        markSlotUsed(t.getRecordId().getTupleNumber(),false);
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException, IOException {
        // some code goes here
        // not necessary for lab1
        if (getNumEmptySlots()==0){
            throw new DbException("there is no empty slots in page");
        }
        if (!t.getTupleDesc().equals(td)){
            throw new DbException("the tupledes is not equal to this");
        }
        for (int i=0;i<numSlots;i++){
            if (!isSlotUsed(i)){
                RecordId rid=new RecordId(pid,i);
                t.setRecordId(rid);
                tuples[i]=t;
                markSlotUsed(i,true);
                //this.markDirty(true,isDirty());
              // Database.getCatalog().getDatabaseFile(pid.tableId).writePage(this);
                return;
            }
        }
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        // some code goes here
	// not necessary for lab1
        this.dirty=dirty;
        if (dirty){
            dirtierTid=tid;
        }
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        // some code goes here
	// Not necessary for lab1
        if (dirty){
            return dirtierTid;
        }
        return null;      
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        // some code goes here
        int numEmpty=0;
        for (int i = 0; i < header.length-1; i++) {
            Byte headerByte=header[i];
            for(int j=0;j<8;j++){
                numEmpty+=(1-(int)(headerByte&0x01));
                headerByte=(byte)(headerByte>>1);
            }
        }
        Byte headerByte=header[header.length-1];
        for(int i=0;i<getNumTuples()-((header.length-1)*8);i++){
            numEmpty+=(1-((int)((headerByte)&0x01)));
            headerByte=(byte)(headerByte>>1);
        }
        return numEmpty;
    }

    /**e if associated slot on this
     *      * Returns tru page is filled.
     */
    public boolean isSlotUsed(int i) {
        // some code goes here
        if((i+1)>numSlots){
            return false;
        }
        i=i+1;
        if(i%8==0){
            int byteNum=Math.floorDiv(i,8);
            Byte slotByte=header[byteNum-1];
            if((slotByte>>7&0x01)==1){
                return true;
            }
            else return false;
        }
        int front=Math.floorDiv(i,8);
        Byte byteSlot=header[front];
        int numBit=i%8;
        if (((byteSlot>>(numBit-1))&0x01)==1){
            return true;
        }
        else return false;
    }
    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        // some code goes here
        // not necessary for lab1
        if (value) {
            i = i + 1;
            if (i % 8 == 0) {
                int byteNum = Math.floorDiv(i, 8);
                header[byteNum - 1] = (byte) (header[byteNum - 1] | 0x80);
            } else {
                int front = Math.floorDiv(i, 8);
                byte byteSlot = header[front];
                int numBit = i % 8;
                byte b = byteSlot;
                byte b1 = (byte) ((byteSlot >> (numBit - 1)) | 0x01);
                b1 = (byte) (b1 << (numBit - 1));
                b = (byte) (b | b1);
                header[front] = b;
            }
        } else {
            i = i + 1;
            if (i % 8 == 0) {
                int byteNum = Math.floorDiv(i, 8);
                header[byteNum - 1] = (byte) (header[byteNum - 1] & 0x7F);
            } else {
                int front = Math.floorDiv(i, 8);
                byte byteSlot = header[front];
                int numBit = i % 8;
                byte b = byteSlot;
                byte b1 = (byte) ((byteSlot >> (numBit - 1)) & 0xFE);
                for (int j = 0; j < numBit-1; j++) {
                    b1 = (byte) ((b1 << 1) | 0x01);
                }
                b = (byte) (b & b1);
                header[front] = b;
            }
        }
        return;
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        // some code goes here
        List<Tuple> tuple2=new ArrayList<>();
        for(int i=0;i<getNumTuples();i++){
            if(tuples[i]!=null){
                tuple2.add(tuples[i]);
            }
        }
        return tuple2.iterator();
    }

}

