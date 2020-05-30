package simpledb;

import java.util.*;

public class PageState {
    private TransactionId tid;
    private int state;
    public PageState(TransactionId tid,int state){
        this.tid=tid;
        this.state=state;
    }
    public TransactionId getTid(){
        return tid;
    }
    public int getState(){
        return state;
    }
    public void setState(int state){
        this.state=state;
    }
}