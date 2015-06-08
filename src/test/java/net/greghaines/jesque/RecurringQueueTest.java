package net.greghaines.jesque;

import net.greghaines.jesque.client.Client;
import net.greghaines.jesque.client.ClientImpl;
import net.greghaines.jesque.utils.JesqueUtils;
import net.greghaines.jesque.worker.MapBasedJobFactory;
import net.greghaines.jesque.worker.Worker;
import net.greghaines.jesque.worker.WorkerImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.Set;

import static net.greghaines.jesque.TestUtils.createJedis;
import static net.greghaines.jesque.utils.JesqueUtils.createKey;
import static net.greghaines.jesque.utils.JesqueUtils.entry;
import static net.greghaines.jesque.utils.JesqueUtils.map;
import static net.greghaines.jesque.utils.ResqueConstants.*;
import static net.greghaines.jesque.utils.ResqueConstants.QUEUE;

/**
 * Created by Karthik (@argvk) on 6/3/15.
 */
public class RecurringQueueTest {

    private static final Config config = new ConfigBuilder().build();
    private static final String recurringTestQueue = "fooRecurring";
    private static final long recurringFrequency = 1000;
    private static final Client client = new ClientImpl(config);
    private static final String queueKey = createKey(config.getNamespace(), QUEUE, recurringTestQueue);
    private static final String hashKey = JesqueUtils.createRecurringHashKey(queueKey);

    @Before
    public void resetRedis() {
        TestUtils.resetRedis(config);
    }

    @Test
    public void testCode() throws Exception {

        Job job = new Job("TestAction", new Object[]{1, 2.3, true, "test", Arrays.asList("inner", 4.5)});
        client.recurringEnqueue(recurringTestQueue, job, System.currentTimeMillis() + recurringFrequency, recurringFrequency);

        Jedis jedis = createJedis(config);
        try { // Assert that we enqueued the job
            Assert.assertEquals(Long.valueOf(1),
                    jedis.zcount(queueKey, "-inf", "+inf"));
            Set<String> jobSet = jedis.zrangeByScore(queueKey, "-inf", "+inf", 0, 1);

            String jobString = jobSet.iterator().next();
            Assert.assertEquals(String.valueOf(recurringFrequency),
                    jedis.hget(hashKey, jobString));
        } finally {
            jedis.quit();
        }

        // Create and mark the start worker
        final Worker worker = new WorkerImpl(config, Arrays.asList(recurringTestQueue),
                new MapBasedJobFactory(map(entry("TestAction", TestAction.class))));
        final Thread workerThread = new Thread(worker);
        Long startMillis = System.currentTimeMillis();
        workerThread.start();

        try { // let the worker run for some time,
            Thread.sleep(1000 * 6);
        } finally { // Stop the worker
            TestUtils.stopWorker(worker, workerThread);
        }

        // stop the recurring queue and mark it as ended
        client.removeRecurringEnqueue(recurringTestQueue, job);
        Long endMillis = System.currentTimeMillis();

        // should have run (startMillis-endMillis/frequency)
        Long times = ((endMillis - startMillis)/recurringFrequency);

        // Assert that the job was run by the worker
        jedis = createJedis(config);
        try {
            Long processedTimes = Long.valueOf(jedis.get(createKey(config.getNamespace(), STAT, PROCESSED)));

            // allowing for off by one error
            Long oneOrZero = Math.abs(times - processedTimes);

            Assert.assertTrue(oneOrZero == 0 || oneOrZero == 1);
            Assert.assertNull(jedis.get(createKey(config.getNamespace(), STAT, FAILED)));
            Assert.assertEquals(Long.valueOf(0),
                    jedis.zcount(createKey(config.getNamespace(), QUEUE, recurringTestQueue), "-inf", "+inf"));
            Assert.assertEquals(Long.valueOf(0), jedis.hlen(hashKey));
        } finally {
            jedis.quit();
        }
    }

    @After
    public void closeClient() {
        client.end();
    }
}
