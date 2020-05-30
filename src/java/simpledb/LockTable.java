package simpledb;
import com.sun.scenario.animation.shared.ClipEnvelope;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class LockTable {
    private Map<PageId, ArrayList<PageState>> pages;

    public LockTable() {
        pages = new ConcurrentHashMap<>();
    }

    public boolean addSlock(PageId pid, TransactionId tid) {
        if (pages.get(pid) != null) {
            ArrayList<PageState> states = pages.get(pid);
            int statesFlag = 0;
            for (int i = 0; i < states.size(); i++) {
                if (states.get(i).getTid() != tid && states.get(i).getState() == -1) {
                    return false;
                }
                if (states.get(i).getTid() == tid) {
                    if (states.get(i).getState() == 1) {
                        statesFlag = 1;//当前事务有读锁
                    }
                    if (states.get(i).getState() == -1) {
                        statesFlag = -1;//当前事务有写锁
                    }
                }
            }
            if (statesFlag == 1) {
                return true;
            } else {
                states.add(new PageState(tid, 1));
                return true;
            }
        }

        PageState pageState = new PageState(tid, 1);
        ArrayList stateList = new ArrayList();
        stateList.add(pageState);
        pages.put(pid, stateList);
        return true;
    }

    public boolean addXlock(PageId pid, TransactionId tid) {
        if (pages.get(pid) != null) {
            ArrayList<PageState> states = pages.get(pid);
            int statesFlag = 0;
            int readLockIndex = 0;
            for (int i = 0; i < states.size(); i++) {//若states中无，则不循环
                if (states.get(i).getTid() != tid) {
                    return false;
                }
                if (states.get(i).getTid() == tid) {
                    if (states.get(i).getState() == 1) {
                        statesFlag = 1;//当前事务有读锁
                        readLockIndex = i;
                    }
                    if (states.get(i).getState() == -1) {
                        statesFlag = -1;//当前事务有写锁
                    }
                }
            }
            if (statesFlag == 1) {
                states.get(readLockIndex).setState(-1);
                return true;
            }
            if (statesFlag == 0) {
                states.add(new PageState(tid, -1));
                return true;
            }
            if (statesFlag==-1){
                return true;
            }
        }
        PageState pageState = new PageState(tid, -1);
        ArrayList stateList = new ArrayList();
        stateList.add(pageState);
        pages.put(pid, stateList);
        return true;
    }
    public void unlock(PageId pid,TransactionId tid){
        ArrayList<PageState> statesList=pages.get(pid);
        if (statesList!=null) {
            for (int i = statesList.size()-1; i>=0; i--) {
                if (statesList.get(i).getTid() == tid) {
                    statesList.remove(i);
                }
            }
        }
    }
    public boolean holdLock(PageId pid,TransactionId tid) {
            if (pages.get(pid)==null){
                return false;
            }
            ArrayList<PageState> statelist=pages.get(pid);
            for (int i=0;i<statelist.size();i++){
                if (statelist.get(i).getTid()==tid){
                    return true;
                }
            }
            return false;
    }
}

