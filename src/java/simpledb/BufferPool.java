package simpledb;

import org.omg.PortableServer.LIFESPAN_POLICY_ID;

import java.io.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    private int numPage;
    private List<Page> pages;
    private LockTable lockTable;
    private ReentrantReadWriteLock rtlock=new ReentrantReadWriteLock();
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPage=numPages;
        pages=new ArrayList<Page>();
        lockTable=new LockTable();
    }

    public static int getPageSize() {
      return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        synchronized (this) {
            if (perm == Permissions.READ_WRITE) {
                boolean result = lockTable.addXlock(pid, tid);
                int roundtimes=0;
                while (!result) {
                    try {
                        if (roundtimes==2){
                            throw new TransactionAbortedException();
                        }
                        Thread.sleep(500);
                        result = lockTable.addXlock(pid, tid);
                        roundtimes++;
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            } else if (perm == Permissions.READ_ONLY) {
                boolean result = lockTable.addSlock(pid, tid);
                int roundtimes=0;
                while (!result) {
                    try {
                        if (roundtimes==1){
                            throw new TransactionAbortedException();
                        }
                        Thread.sleep(500);
                        result = lockTable.addSlock(pid, tid);
                        roundtimes++;
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            for (int i = 0; i < pages.size(); i++) {
                if (pages.get(i).getId().equals(pid)) {
                    return pages.get(i);
                }
            }
            if (pages.size() == numPage) {
                //pages.remove(pages.size()-1);
                evictPage();
            }
            DbFile dbf = Database.getCatalog().getDatabaseFile(pid.getTableId());
            pages.add(dbf.readPage(pid));
            return pages.get(pages.size() - 1);
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockTable.unlock(pid,tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid,true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockTable.holdLock(p,tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2

        for (int i=0;i<pages.size();i++){
            if (pages.get(i).isDirty()==tid){
                if (commit){
                    flushPage(pages.get(i).getId());
                }
                else {
                    DbFile dbf=Database.getCatalog().getDatabaseFile(pages.get(i).getId().getTableId());
                    pages.set(i,dbf.readPage(pages.get(i).getId()));
                }
            }
        }
        for (int i=0;i<pages.size();i++){//应该先解锁再操作，否则若abort，锁
            if (holdsLock(tid,pages.get(i).getId())){
                releasePage(tid,pages.get(i).getId());
            }
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, TransactionAbortedException, IOException {
        // some code goes here
        // not necessary for lab1
        DbFile dbFile= Database.getCatalog().getDatabaseFile(tableId);
        List<Page> page=dbFile.insertTuple(tid,t);
        PageId pageId;
        Page page1;
        if (page.size()>1) {
            int pgNo=page.get(0).getId().getPageNumber();
            if (page.get(0).getClass().equals(HeapPage.class)) {
                for (int i = 0; i < page.size(); i++) {
                    pageId = new HeapPageId(tableId, pgNo + i);
                    byte[] pageDate = page.get(i).getPageData();
                    page1 = new HeapPage((HeapPageId) pageId, pageDate);
                    dbFile.writePage(page1);
                    page.set(i, page1);
                }
            }
            for (int i = 0; i < page.size(); i++) {
                page.get(i).markDirty(true, tid);
//                flushPage(page.get(i).getId());
//                releasePage(tid,page.get(i).getId());
            }
        }
        else {
            page.get(0).markDirty(true,tid);
//            flushPage(page.get(0).getId());
//            releasePage(tid,page.get(0).getId());
        }

    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile dbFile=Database.getCatalog().getDatabaseFile(t.getRecordId().pid.getTableId());
        List<Page> page=dbFile.deleteTuple(tid,t);
        page.get(0).markDirty(true,tid);
        for (int i = 0; i < page.size(); i++) {
            page.get(i).markDirty(true, tid);
            flushPage(page.get(i).getId());
            lockTable.unlock(page.get(i).getId(),tid);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for(int i=0;i<pages.size();i++){
            flushPage(pages.get(i).getId());
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.

        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        int i=0;
        for ( ;i<pages.size();i++){
            if (pages.get(i).getId()==pid)
                pages.remove(i);
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1

        Page page=null;
            for (int i=0;i<pages.size();i++) {
                if (pages.get(i).getId() == pid) {
                    page = pages.get(i);
                }
            }
        if (page.isDirty()!=null) {
            page.markDirty(false,null);
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);

        }
   }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        Page page=null ;
        int index=0;
        for (int i=0;i<pages.size();i++){
            if (pages.get(i).isDirty()==null){
                page = pages.get(i);
                index=i;
            }
        }
        if (page==null){
            throw new DbException("no clean pages in evictpage()");
        }
        else {
            try {
                flushPage(page.getId());
            } catch (IOException e) {
                e.printStackTrace();
            }
            pages.remove(index);
        }
    }

}
