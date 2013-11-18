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
        final WorkerImplFactory factory = new WorkerImplFactory(new ConfigBuilder().build(), queues, jobTypes);
        final WorkerImpl worker = factory.call();
        Assert.assertNotNull(worker);
        Assert.assertEquals(queues.size(), worker.getQueues().size());
        Assert.assertTrue(queues.containsAll(worker.getQueues()));
        Assert.assertEquals(jobTypes.size(), worker.getJobTypes().size());
        Assert.assertTrue(jobTypes.keySet().containsAll(worker.getJobTypes().keySet()));
        Assert.assertTrue(jobTypes.values().containsAll(worker.getJobTypes().values()));
    }
}
