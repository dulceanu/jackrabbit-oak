package org.apache.jackrabbit.oak.segment.scheduler;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

public class ScheduledCommitsQueue implements ScheduledCommits {
    BlockingQueue<Commit> queue = new PriorityBlockingQueue<Commit>();

    @Override
    public Commit next() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void add(Commit commit) {
        // TODO Auto-generated method stub
        
    }
    
    
}
