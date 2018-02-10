/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.intelie.challenges;

import java.util.Iterator;
import java.util.List;

/**
 *
 * @author wallace
 */
public class EventIteratorImpl implements EventIterator {

    //list of selected events in the query
    private final List<Event> events;
    //reference to eventStore from this iterator come
    private final EventStoreImpl eventStore;
    private int cursor = -1;
    
    EventIteratorImpl(EventStoreImpl eventStore, List<Event> events) {
        this.eventStore = eventStore;
        this.events = events;
    }

    @Override
    public boolean moveNext() {
        cursor++;
        if(cursor >= events.size()) {
            return false;
        }
        return true;
    }

    @Override
    public Event current() {
        if(cursor == -1 || cursor >= events.size()) {
            throw new IllegalStateException();
        }
        return events.get(cursor);
    }

    @Override
    public void remove() {
        if(cursor == -1 || cursor >= events.size()) {
            throw new IllegalStateException();
        }
        eventStore.removeEvent(events.get(cursor));
        events.remove(cursor);
    }

    @Override
    public void close() throws Exception {
        eventStore.closeEventIterator(this);
    }
    
    
}
