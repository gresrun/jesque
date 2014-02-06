package net.greghaines.jesque.worker;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import net.greghaines.jesque.ConfigBuilder;
import net.greghaines.jesque.TestAction;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests WorkerImplFactory.
 * 
 * @author Greg Haines
 */
public class TestWorkerImplFactory {

    @Test
    public void testCall() {
        final Collection<String> queues = Arrays.asList("foo", "bar");
        final Map<String,Class<?>> jobTypes = new LinkedHashMap<String,Class<?>>(1);
        jobTypes.put("test", TestAction.class);
        final MapBasedJobFactory jobFactory = new MapBasedJobFactory(jobTypes);
        final WorkerImplFactory factory = new WorkerImplFactory(new ConfigBuilder().build(), queues, jobFactory);
        final WorkerImpl worker = factory.call();
        Assert.assertNotNull(worker);
        Assert.assertEquals(queues.size(), worker.getQueues().size());
        Assert.assertTrue(queues.containsAll(worker.getQueues()));
        Assert.assertEquals(jobFactory, worker.getJobFactory());
    }
}
