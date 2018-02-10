/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.intelie.challenges;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author wallace
 */
public class EventStoreImpl implements EventStore {

    // int this way we can parallelize the insert and removes over different types
    // Moreover the search is faster indexing by the type in a hashmap
    private final HashMap<String, EventList> store = new HashMap();
    private final List<EventIterator> eventIterators = new LinkedList();
    // semaphore must block to only one process adquire in edit function (insert, remove) and allows in the query process
    Semaphore smStore = new Semaphore(Integer.MAX_VALUE);
    Semaphore smEventIterators = new Semaphore(1);

    private EventList getListByType(String type) {
        return store.get(type);

    }

    @Override
    public void insert(Event event) {
        // thread safe to edit the event lists
        try {
            smStore.acquire(Integer.MAX_VALUE);
        } catch (InterruptedException ex) {
            Logger.getLogger(EventStoreImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
        EventList eventList = getListByType(event.type());
        if (eventList == null) {
            eventList = new EventList();
            store.put(event.type(), eventList);
        }
        smStore.release(Integer.MAX_VALUE);
        // end thread safe to edit the event lists
        eventList.insertEvent(event);
    }

    @Override
    public void removeAll(String type) {
        // thread safe to edit the event lists
        try {
            smStore.acquire(Integer.MAX_VALUE);
        } catch (InterruptedException ex) {
            Logger.getLogger(EventStoreImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
        store.remove(type);
        // end of thread safe to edit the event lists
        smStore.release(Integer.MAX_VALUE);
    }

    @Override
    public EventIterator query(String type, long startTime, long endTime) {
        // thread safe to get the Event Lists
        try {
            smStore.acquire(1);
        } catch (InterruptedException ex) {
            Logger.getLogger(EventStoreImpl.class.getName()).log(Level.SEVERE, null, ex);
        }

        EventList eventList = getListByType(type);
        smStore.release(1);
        // end of thread safe to get the Event Lists
        EventIteratorImpl eventIterator;
        if (eventList == null) {
            eventIterator = new EventIteratorImpl(this, new LinkedList());
            // thread safe to edit the list of iterators
            try {
                smEventIterators.acquire();
            } catch (InterruptedException ex) {
                Logger.getLogger(EventStoreImpl.class.getName()).log(Level.SEVERE, null, ex);
            }
            eventIterators.add(eventIterator);
            smEventIterators.release();
            // end of thread safe to edit the list of iterators
            return eventIterator;
        }

        List<Event> list = eventList.query(startTime, endTime);
        eventIterator = new EventIteratorImpl(this, list);
        // thread safe to edit the list of iterators
        try {
            smEventIterators.acquire();
        } catch (InterruptedException ex) {
            Logger.getLogger(EventStoreImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
        eventIterators.add(eventIterator);
        smEventIterators.release();
        // end of thread safe to edit the list of iterators

        return eventIterator;
    }

    protected void closeEventIterator(EventIterator eventIerator) {
        // thread safe to edit the list of iterators
        try {
            smEventIterators.acquire();
        } catch (InterruptedException ex) {
            Logger.getLogger(EventStoreImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
        eventIterators.remove(eventIerator);
        smEventIterators.release();
        // end of thread safe to edit the list of iterators
    }

    protected void removeEvent(Event event) {
        //thread safe to edit the event lists
        try {
            smStore.acquire(Integer.MAX_VALUE);
        } catch (InterruptedException ex) {
            Logger.getLogger(EventStoreImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
        EventList eventList = getListByType(event.type());
        smStore.release(Integer.MAX_VALUE);
        //end thread safe to edit the event lists
        if (eventList != null) {
            eventList.removeEvent(event);
        }
    }

}
