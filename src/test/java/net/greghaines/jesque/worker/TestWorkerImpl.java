package net.greghaines.jesque.worker;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

public class TestWorkerImpl {

    @Test
    public void testSetThreadNameChangingEnabled() {
        WorkerImpl.setThreadNameChangingEnabled(true);
        Assert.assertTrue(WorkerImpl.isThreadNameChangingEnabled());
        WorkerImpl.setThreadNameChangingEnabled(false);
        Assert.assertFalse(WorkerImpl.isThreadNameChangingEnabled());
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testCheckQueues_Null() {
        WorkerImpl.checkQueues(null);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testCheckQueues_NullQueue() {
        WorkerImpl.checkQueues(Arrays.asList("foo", null));
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testCheckQueues_EmptyQueue() {
        WorkerImpl.checkQueues(Arrays.asList("foo", ""));
    }
    
    @Test
    public void testCheckQueues_OK() {
        WorkerImpl.checkQueues(Arrays.asList("foo", "bar"));
    }
}
