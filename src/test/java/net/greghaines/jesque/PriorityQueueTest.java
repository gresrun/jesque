package net.greghaines.jesque;

import net.greghaines.jesque.client.Client;
import net.greghaines.jesque.client.ClientImpl;
import net.greghaines.jesque.utils.JedisUtils;
import net.greghaines.jesque.utils.JesqueUtils;
import net.greghaines.jesque.worker.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static net.greghaines.jesque.TestUtils.createJedis;
import static net.greghaines.jesque.utils.JesqueUtils.*;
import static net.greghaines.jesque.utils.ResqueConstants.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * @author itaim
 */
public class PriorityQueueTest {

    private static final Config config = new ConfigBuilder().build();
    private static final String testQueue = "foo";
    private static final String priorityTestQueue = "fooPriority";

    @Before
    public void resetRedis() {
        TestUtils.resetRedis(config);
    }

    @Test
    public void testCode() throws Exception {

        // Enqueue the job before worker is created and started
        final List<Job> jobs = new ArrayList<Job>(10);
        for (int i = 0; i < 10; i++) {
            jobs.add(new Job("TestAction", new Object[]{i, Double.valueOf(i), true, "test", Arrays.asList("inner", 4.5)}));
        }
        TestUtils.enqueueJobsWithPriority(priorityTestQueue, jobs, config);
        jobs.clear();
        for (int i = 10; i < 20; i++) {
            jobs.add(new Job("TestAction", new Object[]{i, 2.3, true, "test", Arrays.asList("inner", 4.5)}));
        }
        TestUtils.enqueueJobs(testQueue, jobs, config);

        Jedis jedis = createJedis(config);
        try { // Assert that we enqueued the job
            Assert.assertEquals(Long.valueOf(10),
                    jedis.zcount(createKey(config.getNamespace(), QUEUE, priorityTestQueue), "-inf", "+inf"));
        } finally {
            jedis.quit();
        }

        // Create and start worker
        final Worker worker = new WorkerImpl(config, Arrays.asList(priorityTestQueue),
                new MapBasedJobFactory(map(entry("TestAction", TestAction.class))));
        final StringBuilder processOrder = new StringBuilder();
        worker.getWorkerEventEmitter().addListener(new WorkerListener() {
            @Override
            public void onEvent(WorkerEvent event, Worker worker, String queue, Job job, Object runner, Object result, Throwable t) {
                processOrder.append(job.getArgs()[1]).append(",");
            }
        }, WorkerEvent.JOB_PROCESS);
        final Thread workerThread = new Thread(worker);
        workerThread.start();

        // start second thread
        final Worker worker2 = new WorkerImpl(config, Arrays.asList(testQueue),
                new MapBasedJobFactory(map(entry("TestAction", TestAction.class))));
        final Thread workerThread2 = new Thread(worker2);
        workerThread2.start();

        try { // Wait a bit to ensure the worker had time to process the job
            Thread.sleep(3000);
        } finally { // Stop the worker
            TestUtils.stopWorker(worker, workerThread);
        }

        // Assert that the job was run by the worker
        jedis = createJedis(config);
        try {
            Assert.assertEquals(String.valueOf(20), jedis.get(createKey(config.getNamespace(), STAT, PROCESSED)));
            Assert.assertNull(jedis.get(createKey(config.getNamespace(), STAT, FAILED)));
            Assert.assertEquals(Long.valueOf(0),
                    jedis.zcount(createKey(config.getNamespace(), QUEUE, priorityTestQueue), "-inf", "+inf"));
            Assert.assertEquals("9.0,8.0,7.0,6.0,5.0,4.0,3.0,2.0,1.0,0.0,", processOrder.toString());
        } finally {
            jedis.quit();
        }
    }

    @Test
    public void testReassignQueueTypeWhenEmpty() throws Exception {
        final Client client = new ClientImpl(config);
        Job job = new Job("TestAction", new Object[]{1, 2.3, true, "test", Arrays.asList("inner", 4.5)});
        client.enqueue(priorityTestQueue, job, 7.0);

        final String key = JesqueUtils.createKey(config.getNamespace(), QUEUE, priorityTestQueue);
        Jedis jedis = new Jedis(config.getHost(), config.getPort(), config.getTimeout());
        assertThat(JedisUtils.isPriorityQueue(jedis, key), is(true));

        executeWorker(priorityTestQueue, 300);

        client.enqueue(priorityTestQueue, job);
        assertThat(JedisUtils.isRegularQueue(jedis, key), is(true));

        executeWorker(priorityTestQueue, 300);

        client.delayedEnqueue(priorityTestQueue, job, System.currentTimeMillis() + 100);

        assertThat(JedisUtils.isDelayedQueue(jedis, key), is(true));

        executeWorker(priorityTestQueue, 400);

        jedis.close();

    }

    private void executeWorker(String queue, int sleep) throws InterruptedException {
        final Worker worker = new WorkerImpl(config, Arrays.asList(queue),
                new MapBasedJobFactory(map(entry("TestAction", TestAction.class))));
        final Thread workerThread = new Thread(worker);
        workerThread.start();

        Thread.sleep(sleep);
        TestUtils.stopWorker(worker, workerThread);
    }
}