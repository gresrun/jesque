package net.greghaines.jesque;

import com.fasterxml.jackson.core.JsonProcessingException;
import net.greghaines.jesque.json.ObjectMapperFactory;
import net.greghaines.jesque.utils.JesqueUtils;
import net.greghaines.jesque.utils.ResqueConstants;
import net.greghaines.jesque.worker.MapBasedJobFactory;
import net.greghaines.jesque.worker.Worker;
import net.greghaines.jesque.worker.WorkerImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.Arrays;

/**
 * Test job durability:
 * <p/>
 * <ul>
 * <li>A job should be in the in-flight list while being processed, but not afterwards.</li>
 * <li>A job should be requeued when the worker is shut down immediately.</li>
 * <li>A job should <i>not</i> be requeued when the worker shuts down after completing the job.</li>
 * </ul>
 *
 * @author Daniël de Kok <me@danieldk.eu>
 */
public class DurabilityTest {
    private static final Job sleepJob = new Job("SleepAction", (Long) 3000L);

    private static final Config config = new ConfigBuilder().build();

    private static final String testQueue = "foo";

    @Before
    public void resetRedis() {
        TestUtils.resetRedis(config);
    }

    @Test
    public void testNotInterrupted() throws InterruptedException, JsonProcessingException {
        TestUtils.enqueueJobs(testQueue, Arrays.asList(sleepJob), config);

        final Worker worker = new WorkerImpl(config, Arrays.asList(testQueue),
                new MapBasedJobFactory(JesqueUtils.map(JesqueUtils.entry("SleepAction", SleepAction.class))));
        final Thread workerThread = new Thread(worker);
        workerThread.start();

        Thread.sleep(1000);

        Jedis jedis = TestUtils.createJedis(config);
        Assert.assertTrue("In-flight list should have length one when running the job", 1L == jedis.llen(inFlightKey(worker)));
        Assert.assertEquals("Object on the in-flight list should be the serialized job",
                ObjectMapperFactory.get().writeValueAsString(sleepJob), jedis.lindex(inFlightKey(worker), 0));

        TestUtils.stopWorker(worker, workerThread, false);

        Assert.assertTrue("The job should not be requeued after succesful processing", jedis.llen(testQueue) == 0L);
        Assert.assertTrue("In-flight list should be empty when finishing a job", jedis.llen(inFlightKey(worker)) == 0L);
    }

    @Test
    public void testInterrupted() throws InterruptedException, JsonProcessingException {
        TestUtils.enqueueJobs(testQueue, Arrays.asList(sleepJob), config);

        final Worker worker = new WorkerImpl(config, Arrays.asList(testQueue),
                new MapBasedJobFactory(JesqueUtils.map(JesqueUtils.entry("SleepAction", SleepAction.class))));
        final Thread workerThread = new Thread(worker);
        workerThread.start();

        TestUtils.stopWorker(worker, workerThread, true);

        Jedis jedis = TestUtils.createJedis(config);
        Assert.assertTrue("Job was not requeued", jedis.llen(testQueue) == 1L);
        Assert.assertEquals("Incorrect job was requeued", ObjectMapperFactory.get().writeValueAsString(sleepJob),
                jedis.lindex(testQueue, 0));
        Assert.assertTrue("In-flight list should be empty when finishing a job", jedis.llen(inFlightKey(worker)) == 0L);
    }

    private String inFlightKey(Worker worker) {
        return JesqueUtils.createKey(config.getNamespace(), ResqueConstants.INFLIGHT, worker.getName(), testQueue);
    }

}
