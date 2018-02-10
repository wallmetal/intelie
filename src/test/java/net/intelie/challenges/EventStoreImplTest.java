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
public class EventStoreImplTest {

    @Test
    public void simpleInsert() throws Exception {
        Event event = new Event("some_type", 123L);
        EventStoreImpl eventStore = new EventStoreImpl();
        eventStore.insert(event);
        EventIterator eventIterator = eventStore.query("some_type", 0, 125L);
        assertEquals(true, eventIterator.moveNext());
        assertEquals(event, eventIterator.current());
        assertEquals(false, eventIterator.moveNext());
    }
    
    @Test
    public void multipleInsert() throws Exception {
        Event event = new Event("some_type", 123L);
        EventStoreImpl eventStore = new EventStoreImpl();
        eventStore.insert(event);
        event = new Event("some_type", 56);
        eventStore.insert(event);
        event = new Event("some_type", 12);
        eventStore.insert(event);
        event = new Event("some_type", 89);
        eventStore.insert(event);
        event = new Event("some_type", 5);
        eventStore.insert(event);
        event = new Event("some_type", 4);
        eventStore.insert(event);
        event = new Event("some_type", 37);
        eventStore.insert(event);
        EventIterator eventIterator = eventStore.query("some_type", 0, 125L);
        assertEquals(true, eventIterator.moveNext());
        assertEquals(4, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(5, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(12, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(37, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(56, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(89, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(123, eventIterator.current().timestamp());
        assertEquals(false, eventIterator.moveNext());
    }
    
    @Test
    public void multipleInsertWithDuplicatedtimestamp() throws Exception {
        Event event = new Event("some_type", 123L);
        EventStoreImpl eventStore = new EventStoreImpl();
        eventStore.insert(event);
        event = new Event("some_type", 56);
        eventStore.insert(event);
        event = new Event("some_type", 12);
        eventStore.insert(event);
        event = new Event("some_type", 89);
        eventStore.insert(event);
        event = new Event("some_type", 5);
        eventStore.insert(event);
        event = new Event("some_type", 123);
        eventStore.insert(event);
        event = new Event("some_type", 4);
        eventStore.insert(event);
        event = new Event("some_type", 12);
        eventStore.insert(event);
        event = new Event("some_type", 37);
        eventStore.insert(event);
        event = new Event("some_type", 4);
        eventStore.insert(event);
        event = new Event("some_type", 89);
        eventStore.insert(event);
        EventIterator eventIterator = eventStore.query("some_type", 0, 125L);
        assertEquals(true, eventIterator.moveNext());
        assertEquals(4, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(4, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(5, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(12, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(12, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(37, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(56, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(89, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(89, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(123, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(123, eventIterator.current().timestamp());
        assertEquals(false, eventIterator.moveNext());
    }
    
    @Test
    public void removeAll() throws Exception {
        Event event = new Event("some_type", 123L);
        EventStoreImpl eventStore = new EventStoreImpl();
        eventStore.insert(event);
        event = new Event("some_type", 56);
        eventStore.insert(event);
        event = new Event("some_type", 12);
        eventStore.insert(event);
        event = new Event("some_type", 89);
        eventStore.insert(event);
        event = new Event("some_type", 5);
        eventStore.insert(event);
        event = new Event("some_type", 4);
        eventStore.insert(event);
        event = new Event("some_type", 37);
        eventStore.insert(event);
        eventStore.removeAll("some_type");
        EventIterator eventIterator = eventStore.query("some_type", 0, 125L);
        assertEquals(false, eventIterator.moveNext());
    }
    
    @Test
    public void removeAllFromAnotherType() throws Exception {
        Event event = new Event("some_type", 123L);
        EventStoreImpl eventStore = new EventStoreImpl();
        eventStore.insert(event);
        event = new Event("some_type", 56);
        eventStore.insert(event);
        event = new Event("some_type", 12);
        eventStore.insert(event);
        event = new Event("some_type", 89);
        eventStore.insert(event);
        event = new Event("some_type", 5);
        eventStore.insert(event);
        event = new Event("some_type", 4);
        eventStore.insert(event);
        event = new Event("some_type", 37);
        eventStore.insert(event);
        eventStore.removeAll("som_type");
        EventIterator eventIterator = eventStore.query("some_type", 0, 125L);
        assertEquals(true, eventIterator.moveNext());
        assertEquals(4, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(5, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(12, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(37, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(56, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(89, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(123, eventIterator.current().timestamp());
        assertEquals(false, eventIterator.moveNext());
    }
    
    @Test
    public void query() throws Exception {
        Event event = new Event("some_type", 123L);
        EventStoreImpl eventStore = new EventStoreImpl();
        eventStore.insert(event);
        event = new Event("some_type", 56);
        eventStore.insert(event);
        event = new Event("some_type", 12);
        eventStore.insert(event);
        event = new Event("some_type", 89);
        eventStore.insert(event);
        event = new Event("some_type", 5);
        eventStore.insert(event);
        event = new Event("some_type", 123);
        eventStore.insert(event);
        event = new Event("some_type", 4);
        eventStore.insert(event);
        event = new Event("some_type", 12);
        eventStore.insert(event);
        event = new Event("some_type", 37);
        eventStore.insert(event);
        event = new Event("some_type", 4);
        eventStore.insert(event);
        event = new Event("some_type", 89);
        eventStore.insert(event);
        EventIterator eventIterator = eventStore.query("some_type", 0, 125L);
        assertEquals(true, eventIterator.moveNext());
        assertEquals(4, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(4, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(5, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(12, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(12, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(37, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(56, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(89, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(89, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(123, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(123, eventIterator.current().timestamp());
        assertEquals(false, eventIterator.moveNext());
    }
    
    @Test
    public void queryWithDuplicatedExcludeAtEnd() throws Exception {
        Event event = new Event("some_type", 123L);
        EventStoreImpl eventStore = new EventStoreImpl();
        eventStore.insert(event);
        event = new Event("some_type", 56);
        eventStore.insert(event);
        event = new Event("some_type", 12);
        eventStore.insert(event);
        event = new Event("some_type", 89);
        eventStore.insert(event);
        event = new Event("some_type", 5);
        eventStore.insert(event);
        event = new Event("some_type", 123);
        eventStore.insert(event);
        event = new Event("some_type", 4);
        eventStore.insert(event);
        event = new Event("some_type", 12);
        eventStore.insert(event);
        event = new Event("some_type", 37);
        eventStore.insert(event);
        event = new Event("some_type", 4);
        eventStore.insert(event);
        event = new Event("some_type", 89);
        eventStore.insert(event);
        event = new Event("some_type", 89);
        eventStore.insert(event);
        event = new Event("some_type", 89);
        eventStore.insert(event);
        event = new Event("some_type", 89);
        eventStore.insert(event);
        EventIterator eventIterator = eventStore.query("some_type", 0, 89);
        assertEquals(true, eventIterator.moveNext());
        assertEquals(4, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(4, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(5, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(12, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(12, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(37, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(56, eventIterator.current().timestamp());
        assertEquals(false, eventIterator.moveNext());
    }
    
    @Test
    public void queryWithDuplicatedIncludeAtEnd() throws Exception {
        Event event = new Event("some_type", 123L);
        EventStoreImpl eventStore = new EventStoreImpl();
        eventStore.insert(event);
        event = new Event("some_type", 56);
        eventStore.insert(event);
        event = new Event("some_type", 12);
        eventStore.insert(event);
        event = new Event("some_type", 89);
        eventStore.insert(event);
        event = new Event("some_type", 5);
        eventStore.insert(event);
        event = new Event("some_type", 123);
        eventStore.insert(event);
        event = new Event("some_type", 4);
        eventStore.insert(event);
        event = new Event("some_type", 12);
        eventStore.insert(event);
        event = new Event("some_type", 37);
        eventStore.insert(event);
        event = new Event("some_type", 4);
        eventStore.insert(event);
        event = new Event("some_type", 89);
        eventStore.insert(event);
        event = new Event("some_type", 89);
        eventStore.insert(event);
        event = new Event("some_type", 89);
        eventStore.insert(event);
        event = new Event("some_type", 89);
        eventStore.insert(event);
        event = new Event("some_type", 88);
        eventStore.insert(event);
        event = new Event("some_type", 88);
        eventStore.insert(event);
        event = new Event("some_type", 88);
        eventStore.insert(event);
        event = new Event("some_type", 88);
        eventStore.insert(event);
        event = new Event("some_type", 88);
        eventStore.insert(event);
        event = new Event("some_type", 88);
        eventStore.insert(event);
        EventIterator eventIterator = eventStore.query("some_type", 0, 89);
        assertEquals(true, eventIterator.moveNext());
        assertEquals(4, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(4, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(5, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(12, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(12, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(37, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(56, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(88, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(88, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(88, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(88, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(88, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(88, eventIterator.current().timestamp());
        assertEquals(false, eventIterator.moveNext());
    }
    
    @Test
    public void queryWithDuplicatedAtStart() throws Exception {
        Event event = new Event("some_type", 123L);
        EventStoreImpl eventStore = new EventStoreImpl();
        eventStore.insert(event);
        event = new Event("some_type", 56);
        eventStore.insert(event);
        event = new Event("some_type", 12);
        eventStore.insert(event);
        event = new Event("some_type", 89);
        eventStore.insert(event);
        event = new Event("some_type", 5);
        eventStore.insert(event);
        event = new Event("some_type", 123);
        eventStore.insert(event);
        event = new Event("some_type", 4);
        eventStore.insert(event);
        event = new Event("some_type", 12);
        eventStore.insert(event);
        event = new Event("some_type", 37);
        eventStore.insert(event);
        event = new Event("some_type", 4);
        eventStore.insert(event);
        event = new Event("some_type", 89);
        eventStore.insert(event);
        event = new Event("some_type", 89);
        eventStore.insert(event);
        event = new Event("some_type", 89);
        eventStore.insert(event);
        event = new Event("some_type", 89);
        eventStore.insert(event);
        event = new Event("some_type", 90);
        eventStore.insert(event);
        event = new Event("some_type", 92);
        eventStore.insert(event);
        EventIterator eventIterator = eventStore.query("some_type", 89, 123);
        assertEquals(true, eventIterator.moveNext());
        assertEquals(89, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(89, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(89, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(89, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(89, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(90, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(92, eventIterator.current().timestamp());
        assertEquals(false, eventIterator.moveNext());
    }
    
    @Test
    public void queryWithDuplicatedAtOneBeforeStart() throws Exception {
        Event event = new Event("some_type", 123L);
        EventStoreImpl eventStore = new EventStoreImpl();
        eventStore.insert(event);
        event = new Event("some_type", 56);
        eventStore.insert(event);
        event = new Event("some_type", 12);
        eventStore.insert(event);
        event = new Event("some_type", 89);
        eventStore.insert(event);
        event = new Event("some_type", 5);
        eventStore.insert(event);
        event = new Event("some_type", 123);
        eventStore.insert(event);
        event = new Event("some_type", 4);
        eventStore.insert(event);
        event = new Event("some_type", 12);
        eventStore.insert(event);
        event = new Event("some_type", 37);
        eventStore.insert(event);
        event = new Event("some_type", 4);
        eventStore.insert(event);
        event = new Event("some_type", 89);
        eventStore.insert(event);
        event = new Event("some_type", 89);
        eventStore.insert(event);
        event = new Event("some_type", 89);
        eventStore.insert(event);
        event = new Event("some_type", 89);
        eventStore.insert(event);
        event = new Event("some_type", 91);
        eventStore.insert(event);
        event = new Event("some_type", 92);
        eventStore.insert(event);
        EventIterator eventIterator = eventStore.query("some_type", 90, 123);
        assertEquals(true, eventIterator.moveNext());
        assertEquals(91, eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(92, eventIterator.current().timestamp());
        assertEquals(false, eventIterator.moveNext());
    }
}
