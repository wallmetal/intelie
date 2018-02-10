/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.intelie.challenges;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author wallace
 */
public class EventList {
    private final List<Event> events = new LinkedList();
    // to query, must be allowed to parallelize. In the edition, must block all
    private final Semaphore sm = new Semaphore(Integer.MAX_VALUE);
    
    private  int binarySearchIndex(long timestamp) {
        if(events.isEmpty()) {
            return 0;
        }
        int begin = 0;
        int end = events.size() - 1;
        int middle;
        while(end > begin) {
            middle = (end + begin)/2;
            long searchTimestamp = events.get(middle).timestamp();
            if(timestamp == searchTimestamp){
                return middle;
            } else if (timestamp > searchTimestamp) {
                begin = middle+1;
            } else {
                end = middle - 1;
            }
        }
        if(events.get(begin).timestamp() > timestamp) {
            return begin;
        }
        return begin + 1;
    }
    
    public void insertEvent(Event event) {
        //thread safe to edit the list
        try {
            sm.acquire(Integer.MAX_VALUE);
        } catch (InterruptedException ex) {
            Logger.getLogger(EventStoreImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
        events.add(binarySearchIndex(event.timestamp()), event);
        sm.release(Integer.MAX_VALUE);
        //end thread safe to edit the list
    }
    
    public void removeEvent(Event event) {
        // thread safe to edit the list
        try {
            sm.acquire(Integer.MAX_VALUE);
        } catch (InterruptedException ex) {
            Logger.getLogger(EventStoreImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
        events.remove(event);
        sm.release(Integer.MAX_VALUE);
        // end thread safe to edit the list
    }
    
    public  List query(long startTime, long endTime) {
        // thread safe to get the current state of thee list
        try {
            sm.acquire(1);
        } catch (InterruptedException ex) {
            Logger.getLogger(EventStoreImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
        int indexBegin = binarySearchIndex(startTime);
        int indexEnd = binarySearchIndex(endTime-1);
        boolean flag = false;
        while(!flag){
            if(indexBegin > 0 && events.get(indexBegin-1).timestamp() == startTime) {
                indexBegin--;
            } else {
                flag = true;
            }
        }
        flag = false;
        int length = events.size();
        while(!flag){
            if(indexEnd < length && events.get(indexEnd).timestamp() == (endTime-1)) {
                indexEnd++;
            } else {
                flag = true;
            }
        }
        List<Event> list = new LinkedList(events.subList(indexBegin, indexEnd));
        sm.release(1);
        // end thread safe to get the current state of thee list
        
        return list;
    }
    
    
}
