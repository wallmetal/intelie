/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.intelie.challenges;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
/**
 *
 * @author wallace
 */
public class EventIteratorImplTest {

    @Test
    public void moveNextAndCurrentWithNotEmptyList() throws Exception {
        Event event = new Event("type1", 0);
        EventStoreImpl eventStore = new EventStoreImpl();
        eventStore.insert(event);
        EventIteratorImpl eventIterator = (EventIteratorImpl) eventStore.query("type1", 0, 125L);
        assertEquals(true, eventIterator.moveNext());
        assertEquals(event, eventIterator.current());
        assertEquals(false, eventIterator.moveNext());
    }
    
    @Test
    public void moveNextAndCurrentWithEmptyList() throws Exception {
        EventStoreImpl eventStore = new EventStoreImpl();
        EventIteratorImpl eventIterator = (EventIteratorImpl) eventStore.query("some_type", 0, 125L);
        assertEquals(false, eventIterator.moveNext());
    }
    
    @Test
    public void current() throws Exception {
        Event event = new Event("type1", 0);
        EventStoreImpl eventStore = new EventStoreImpl();
        eventStore.insert(event);
        event = new Event("type1", 1);
        eventStore.insert(event);
        event = new Event("type1", 2);
        eventStore.insert(event);
        event = new Event("type1", 3);
        eventStore.insert(event);
        EventIteratorImpl eventIterator = (EventIteratorImpl) eventStore.query("type1", 0, 125L);
        assertEquals(true, eventIterator.moveNext());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(1, eventIterator.current().timestamp());
        assertEquals(1, eventIterator.current().timestamp());
        assertEquals(1, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(3, eventIterator.current().timestamp());
        assertEquals(3, eventIterator.current().timestamp());
        assertEquals(3, eventIterator.current().timestamp());
        assertEquals(3, eventIterator.current().timestamp());
    }
    
    @Test
    public void currentWithExceptionBecauseDontMoveNext() throws Exception {
        Event event = new Event("type1", 0);
        EventStoreImpl eventStore = new EventStoreImpl();
        eventStore.insert(event);
        event = new Event("type1", 1);
        eventStore.insert(event);
        event = new Event("type1", 2);
        eventStore.insert(event);
        event = new Event("type1", 3);
        eventStore.insert(event);
        EventIteratorImpl eventIterator = (EventIteratorImpl) eventStore.query("type1", 0, 125L);
        try{
            eventIterator.current();
        }catch(Exception e) {
            assertEquals(true, e instanceof IllegalStateException);
        }
    }
    
    @Test
    public void currentWithExceptionBecauseEnds() throws Exception {
        Event event = new Event("type1", 0);
        EventStoreImpl eventStore = new EventStoreImpl();
        eventStore.insert(event);
        event = new Event("type1", 1);
        eventStore.insert(event);
        event = new Event("type1", 2);
        eventStore.insert(event);
        event = new Event("type1", 3);
        eventStore.insert(event);
        EventIteratorImpl eventIterator = (EventIteratorImpl) eventStore.query("type1", 0, 125L);
        assertEquals(true, eventIterator.moveNext());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(false, eventIterator.moveNext());
        try{
            eventIterator.current();
        }catch(Exception e) {
            assertEquals(true, e instanceof IllegalStateException);
        }
    }
    
    @Test
    public void remove() throws Exception {
        Event event = new Event("type1", 0);
        EventStoreImpl eventStore = new EventStoreImpl();
        eventStore.insert(event);
        event = new Event("type1", 1);
        eventStore.insert(event);
        event = new Event("type1", 2);
        eventStore.insert(event);
        event = new Event("type1", 3);
        eventStore.insert(event);
        EventIteratorImpl eventIterator = (EventIteratorImpl) eventStore.query("type1", 0, 125L);
        assertEquals(true, eventIterator.moveNext());
        assertEquals(0, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(1, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(2, eventIterator.current().timestamp());
        eventIterator.remove();
        assertEquals(3, eventIterator.current().timestamp());
        assertEquals(false, eventIterator.moveNext());
        eventIterator.close();
        eventIterator = (EventIteratorImpl) eventStore.query("type1", 0, 125L);
        assertEquals(true, eventIterator.moveNext());
        assertEquals(0, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(1, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(3, eventIterator.current().timestamp());
        assertEquals(false, eventIterator.moveNext());
    }
    
    @Test
    public void removeFromBegin() throws Exception {
        Event event = new Event("type1", 0);
        EventStoreImpl eventStore = new EventStoreImpl();
        eventStore.insert(event);
        event = new Event("type1", 1);
        eventStore.insert(event);
        event = new Event("type1", 2);
        eventStore.insert(event);
        event = new Event("type1", 3);
        eventStore.insert(event);
        EventIteratorImpl eventIterator = (EventIteratorImpl) eventStore.query("type1", 0, 125L);
        assertEquals(true, eventIterator.moveNext());
        eventIterator.remove();
        assertEquals(1, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(2, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(3, eventIterator.current().timestamp());
        assertEquals(false, eventIterator.moveNext());
        eventIterator.close();
        eventIterator = (EventIteratorImpl) eventStore.query("type1", 0, 125L);
        assertEquals(true, eventIterator.moveNext());
        assertEquals(1, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(2, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(3, eventIterator.current().timestamp());
        assertEquals(false, eventIterator.moveNext());
    }
    
    @Test
    public void removeFromBeginWithException() throws Exception {
        Event event = new Event("type1", 0);
        EventStoreImpl eventStore = new EventStoreImpl();
        eventStore.insert(event);
        event = new Event("type1", 1);
        eventStore.insert(event);
        event = new Event("type1", 2);
        eventStore.insert(event);
        event = new Event("type1", 3);
        eventStore.insert(event);
        EventIteratorImpl eventIterator = (EventIteratorImpl) eventStore.query("type1", 0, 125L);
        try{
            eventIterator.remove();
        }catch(Exception e) {
            assertEquals(true, e instanceof IllegalStateException);
        }
        assertEquals(true, eventIterator.moveNext());
        assertEquals(0, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(1, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(2, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(3, eventIterator.current().timestamp());
        assertEquals(false, eventIterator.moveNext());
        eventIterator.close();
        eventIterator = (EventIteratorImpl) eventStore.query("type1", 0, 125L);
        assertEquals(true, eventIterator.moveNext());
        assertEquals(0, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(1, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(2, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(3, eventIterator.current().timestamp());
        assertEquals(false, eventIterator.moveNext());
    }
    
    @Test
    public void removeFromEnd() throws Exception {
        Event event = new Event("type1", 0);
        EventStoreImpl eventStore = new EventStoreImpl();
        eventStore.insert(event);
        event = new Event("type1", 1);
        eventStore.insert(event);
        event = new Event("type1", 2);
        eventStore.insert(event);
        event = new Event("type1", 3);
        eventStore.insert(event);
        EventIteratorImpl eventIterator = (EventIteratorImpl) eventStore.query("type1", 0, 125L);
        assertEquals(true, eventIterator.moveNext());
        assertEquals(0, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(1, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(2, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(3, eventIterator.current().timestamp());
        eventIterator.remove();
        assertEquals(false, eventIterator.moveNext());
        eventIterator.close();
        eventIterator = (EventIteratorImpl) eventStore.query("type1", 0, 125L);
        assertEquals(true, eventIterator.moveNext());
        assertEquals(0, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(1, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(2, eventIterator.current().timestamp());
        assertEquals(false, eventIterator.moveNext());
    }
    
    @Test
    public void removeFromEndWithException() throws Exception {
        Event event = new Event("type1", 0);
        EventStoreImpl eventStore = new EventStoreImpl();
        eventStore.insert(event);
        event = new Event("type1", 1);
        eventStore.insert(event);
        event = new Event("type1", 2);
        eventStore.insert(event);
        event = new Event("type1", 3);
        eventStore.insert(event);
        EventIteratorImpl eventIterator = (EventIteratorImpl) eventStore.query("type1", 0, 125L);
        assertEquals(true, eventIterator.moveNext());
        assertEquals(0, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(1, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(2, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(3, eventIterator.current().timestamp());
        assertEquals(false, eventIterator.moveNext());
        try{
            eventIterator.remove();
        }catch(Exception e) {
            assertEquals(true, e instanceof IllegalStateException);
        }
        eventIterator.close();
        eventIterator = (EventIteratorImpl) eventStore.query("type1", 0, 125L);
        assertEquals(true, eventIterator.moveNext());
        assertEquals(0, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(1, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(2, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(3, eventIterator.current().timestamp());
        assertEquals(false, eventIterator.moveNext());
    }
}
