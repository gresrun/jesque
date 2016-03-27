package net.greghaines.jesque.worker;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.ConfigBuilder;
import net.greghaines.jesque.TestAction;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.internal.InvocationExpectation;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static net.greghaines.jesque.utils.JesqueUtils.entry;
import static net.greghaines.jesque.utils.JesqueUtils.map;

public class TestWorkerImpl {
    
    private static final Config CONFIG = new ConfigBuilder().build();

    @Test(expected=IllegalArgumentException.class)
    public void testConstructor_NullConfig() {
        new WorkerImpl(null, null, null, null);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testConstructor_NullJobFactory() {
        new WorkerImpl(CONFIG, null, null, null);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testConstructor_NullJedis() {
        new WorkerImpl(CONFIG, null, new MapBasedJobFactory(map(entry("Test", TestAction.class))), null);
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

    @Test
    public void verifyNoExceptionsForAllNextQueueStrategies() throws InterruptedException {
        final MapBasedJobFactory jobFactory = new MapBasedJobFactory(Collections.EMPTY_MAP);
        for (WorkerImpl.NextQueueStrategy nextQueueStrategy : WorkerImpl.NextQueueStrategy.values()) {
            final WorkerImpl worker = new WorkerImpl(CONFIG,
                    new ArrayList<String>(),
                    jobFactory,
                    getJedis(),
                    nextQueueStrategy);
            worker.pop(worker.getNextQueue());
        }
    }

    private Jedis getJedis() {
        final Mockery mockCtx = new JUnit4Mockery();
        mockCtx.setImposteriser(ClassImposteriser.INSTANCE);
        final Jedis jedis = mockCtx.mock(Jedis.class);
        mockCtx.addExpectation(new InvocationExpectation());
        return jedis;
    }
}
