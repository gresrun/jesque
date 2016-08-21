package net.greghaines.jesque.worker;

import static net.greghaines.jesque.utils.JesqueUtils.entry;
import static net.greghaines.jesque.utils.JesqueUtils.map;

import java.util.ArrayList;
import java.util.Arrays;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.ConfigBuilder;
import net.greghaines.jesque.TestAction;

import org.junit.Assert;
import org.junit.Test;

public class TestWorkerImpl {
    
    private static final Config CONFIG = new ConfigBuilder().build();
    
    @Test(expected=IllegalArgumentException.class)
    public void testConstructor_NullConfig() {
        new WorkerImpl(null, new ArrayList<String>(), new MapBasedJobFactory(map(entry("Test", TestAction.class))), null, "");
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testConstructor_NullJobFactory() {
        new WorkerImpl(CONFIG, null, null, "");
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testConstructor_NullJedis() {
        new WorkerImpl(CONFIG, null, new MapBasedJobFactory(map(entry("Test", TestAction.class))), null, "");
    }

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
