package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */

public class HeapFile implements DbFile {

    private File file;
    private TupleDesc td;
    private final int id;
    private byte[] b;

    public class HeapfileIterator implements DbFileIterator {
        Iterator<Tuple> tupleIterator;
        List<Tuple> tuples;
        Iterator pageIterator = null;
        TransactionId tid;
        int pgno = 0;

        public HeapfileIterator(TransactionId tid) {
            tupleIterator = null;
            tuples = new ArrayList<>();
            this.tid = tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            if (tuples == null) {
                throw new DbException("tuples of file==null");
            }
            BufferPool bfp = Database.getBufferPool();
            HeapPageId hpid = new HeapPageId(getId(), pgno);
            HeapPage page = null;
            try {
                page = (HeapPage) bfp.getPage(tid, hpid, Permissions.READ_ONLY);
                pgno++;
                pageIterator = page.iterator();
            } catch (TransactionAbortedException e) {
                e.printStackTrace();
            } catch (DbException e) {
                e.printStackTrace();
            }
            while (pageIterator.hasNext()) {
                tuples.add((Tuple) pageIterator.next());
            }
        tupleIterator=tuples.iterator();
    }
        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(tupleIterator==null){
                return false;
            }
            if (!tupleIterator.hasNext()&&(pgno<numPages()))
            {
                tuples.clear();
                BufferPool bfp = Database.getBufferPool();
                HeapPageId hpid = new HeapPageId(getId(), pgno);
                HeapPage page = null;
                try {
                    page = (HeapPage) bfp.getPage(tid, hpid, Permissions.READ_ONLY);
                    pgno++;
                    pageIterator = page.iterator();
                } catch (TransactionAbortedException e) {
                    e.printStackTrace();
                } catch (DbException e) {
                    e.printStackTrace();
                }
                while (pageIterator.hasNext()) {
                    tuples.add((Tuple) pageIterator.next());
                }
                tupleIterator=tuples.iterator();
            }
            return tupleIterator.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(tupleIterator==null){
                throw new NoSuchElementException();
            }
            if(!tupleIterator.hasNext()){
                throw new NoSuchElementException();
            }
            return tupleIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if(tupleIterator==null){
                throw new DbException("cannot rewind the Heapfileiterator");
            }
            close();
            open();
        }

        @Override
        public void close() {
            tupleIterator=null;
            pgno=0;
            tuples.clear();
        }
    }

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td)  {
        // some code goes here
        file=f;
        this.td=td;
        id=f.getAbsoluteFile().hashCode();
        b=new byte[(int) f.length()];
        try{
            FileInputStream fis=new FileInputStream(file);
            fis.read(b);
        }
        catch(IOException e){

        }

    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return id;
//        throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
//        throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid)  {
        // some code goes here
        if(pid.getTableId()!=getId()){
            throw new IllegalArgumentException();
        }

        int off;
        byte[] pageByte=new byte[BufferPool.getPageSize()];
        off=(pid.getPageNumber())*(BufferPool.getPageSize());

        for(int i=0;i<BufferPool.getPageSize();i++){
            pageByte[i]=b[off+i];
        }
        HeapPageId hpid=new HeapPageId(pid.getTableId(),pid.getPageNumber());
        HeapPage page=null ;
        try {
            page = new HeapPage(hpid, pageByte);
        }
        catch(IOException e){

        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {//还需要改变file！！！！！！
        // some code goes here
        // not necessary for lab1
        byte[] b=this.b;
        if (page.getId().getPageNumber()+1>numPages()){
            this.b=Arrays.copyOf(b,BufferPool.getPageSize()*(numPages()+1));
        }
        int off=(page.getId().getPageNumber())*BufferPool.getPageSize();
        byte[]pagebyte=page.getPageData();
        for (int i=0;i<BufferPool.getPageSize();i++){
            this.b[i+off]=pagebyte[i];
        }
        FileOutputStream fis=new FileOutputStream(file);
        fis.write(this.b);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return b.length/BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        BufferPool bfp=Database.getBufferPool();
        HeapPageId hpid;
        HeapPage page=null;
        boolean flag=false;
        for (int i=0;i<numPages();i++){
            hpid=new HeapPageId(getId(),i);
            page=(HeapPage)(bfp.getPage(tid,hpid,Permissions.READ_WRITE));
            if (page.getNumEmptySlots()==0){
                Database.getBufferPool().releasePage(tid,hpid);
            }
            if (page.getNumEmptySlots()!=0){
                page.insertTuple(t);
                flag=true;
                break;
            }
        }
        if (!flag){
            hpid=new HeapPageId(getId(),numPages());
            page=new HeapPage(hpid,HeapPage.createEmptyPageData());
            page.insertTuple(t);
            writePage(page);
        }
//        writePage(page);
        ArrayList<Page> arrayList=new ArrayList<>();
        arrayList.add(page);
//        Database.getBufferPool().releasePage(tid,page.getId());
        return arrayList;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException, IOException {
        // some code goes here
        // not necessary for lab1
        BufferPool bfp=Database.getBufferPool();
        if (t.getRecordId().getPageId().getTableId()!=getId()){
            throw new DbException("this tuple do not belong to this file");
        }
        HeapPageId hpid=new HeapPageId(getId(),t.getRecordId().pid.getPageNumber());
        HeapPage page=(HeapPage) bfp.getPage(tid,hpid,Permissions.READ_WRITE);
        page.deleteTuple(t);
        writePage(page);
        ArrayList<Page> arrayList=new ArrayList<>();
        arrayList.add(page);
        return arrayList;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        HeapfileIterator it=new HeapfileIterator(tid);
        return it;
    }

}

