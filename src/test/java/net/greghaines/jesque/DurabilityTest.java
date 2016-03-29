package net.greghaines.jesque;

import com.fasterxml.jackson.core.JsonProcessingException;
import net.greghaines.jesque.json.ObjectMapperFactory;
import net.greghaines.jesque.utils.JesqueUtils;
import net.greghaines.jesque.worker.MapBasedJobFactory;
import net.greghaines.jesque.worker.Worker;
import net.greghaines.jesque.worker.WorkerImpl;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.Arrays;

import static net.greghaines.jesque.utils.ResqueConstants.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Test job durability:
 * <ul>
 * <li>A job should be in the in-flight list while being processed, but not afterwards.</li>
 * <li>A job should be re-queued when the worker is shut down immediately.</li>
 * <li>A job should <i>not</i> be re-queued when the worker shuts down after completing the job.</li>
 * </ul>
 *
 * @author Daniël de Kok
 */
public class DurabilityTest {
    
    private static final Job sleepJob = new Job("SleepAction", 3000L);
    private static final Config config = new ConfigBuilder().build();

    @Before
    public void resetRedis() {
        TestUtils.resetRedis(config);
    }

    @Test
    public void testDontPerform() throws Exception {
        final String queue = "foo";
        TestUtils.enqueueJobs(queue, Arrays.asList(new Job("SleepAction", 1L)), config);

        final Worker worker = new WorkerImpl(config, Arrays.asList(queue),
                new MapBasedJobFactory(JesqueUtils.map(JesqueUtils.entry("SleepAction", SleepAction.class))));
        worker.getWorkerEventEmitter().addListener(new DontPerformWorkerListener());
        final Thread workerThread = new Thread(worker);
        workerThread.start();

        Thread.sleep(1000);

        final Jedis jedis = TestUtils.createJedis(config);
        assertEquals("The job is not in flight anymore",
                0, (long) jedis.llen(inFlightKey(worker, queue)));
        assertNull("The job did not succeed", jedis.get(statKey(PROCESSED)));
        assertNull("Job did not fail", jedis.get(statKey(FAILED)));
        assertEquals("The job should have been removed from the queue",
                0, (long) jedis.llen(queueKey(queue)));

        worker.end(false);
    }

    private static String statKey(String key) {
        return JesqueUtils.createKey(config.getNamespace(), STAT, key);
    }


    @Test
    public void testNotInterrupted() throws InterruptedException, JsonProcessingException {
        final String queue = "foo";
        TestUtils.enqueueJobs(queue, Arrays.asList(sleepJob), config);

        final Worker worker = new WorkerImpl(config, Arrays.asList(queue),
                new MapBasedJobFactory(JesqueUtils.map(JesqueUtils.entry("SleepAction", SleepAction.class))));
        final Thread workerThread = new Thread(worker);
        workerThread.start();

        Thread.sleep(1000);

        final Jedis jedis = TestUtils.createJedis(config);
        assertEquals("In-flight list should have length one when running the job",
                1, (long) jedis.llen(inFlightKey(worker, queue)));
        assertEquals("Object on the in-flight list should be the first job",
                ObjectMapperFactory.get().writeValueAsString(sleepJob),
                jedis.lindex(inFlightKey(worker, queue), 0));

        TestUtils.stopWorker(worker, workerThread, false);

        assertEquals("The job should not be requeued after successful processing",
                0, (long) jedis.llen(queueKey(queue)));
        assertEquals("In-flight list should be empty when finishing a job",
                0, (long) jedis.llen(inFlightKey(worker, queue)));
    }

    @Test
    public void testInterrupted() throws InterruptedException, JsonProcessingException {
        final String queue = "bar";
        TestUtils.enqueueJobs(queue, Arrays.asList(sleepJob), config);

        final Worker worker = new WorkerImpl(config, Arrays.asList(queue),
                new MapBasedJobFactory(JesqueUtils.map(JesqueUtils.entry("SleepAction", SleepAction.class))));
        final Thread workerThread = new Thread(worker);
        workerThread.start();

        TestUtils.stopWorker(worker, workerThread, true);

        final Jedis jedis = TestUtils.createJedis(config);
        assertEquals("Job should be requeued",
                1, (long) jedis.llen(queueKey(queue)));
        assertEquals("Incorrect job was requeued", ObjectMapperFactory.get().writeValueAsString(sleepJob),
                jedis.lindex(queueKey(queue), 0));
        assertEquals("In-flight list should be empty when finishing a job",
                0, (long) jedis.llen(inFlightKey(worker, queue)));
    }

    @Test
    public void testJSONException() throws InterruptedException, JsonProcessingException {
        final String queue = "baz";

        final Jedis jedis = TestUtils.createJedis(config);

        // Submit a job containing incorrect JSON.
        String incorrectJson = "{";
        jedis.sadd(JesqueUtils.createKey(config.getNamespace(), QUEUES), queue);
        jedis.rpush(queueKey(queue), incorrectJson);

        final Worker worker = new WorkerImpl(config, Arrays.asList(queue),
                new MapBasedJobFactory(JesqueUtils.map(JesqueUtils.entry("SleepAction", SleepAction.class))));
        final Thread workerThread = new Thread(worker);
        workerThread.start();

        Thread.sleep(1000);

        TestUtils.stopWorker(worker, workerThread, false);

        assertEquals("In-flight list should have length zero if the worker failed during JSON deserialization",
                0, (long) jedis.llen(inFlightKey(worker, queue)));
    }

    private static String queueKey(final String queue) {
        return JesqueUtils.createKey(config.getNamespace(), QUEUE, queue);
    }

    private static String inFlightKey(final Worker worker, final String queue) {
        return JesqueUtils.createKey(config.getNamespace(), INFLIGHT, worker.getName(), queue);
    }
}
