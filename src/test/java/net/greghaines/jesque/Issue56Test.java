package net.greghaines.jesque;

import static net.greghaines.jesque.utils.JesqueUtils.entry;
import static net.greghaines.jesque.utils.JesqueUtils.map;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import net.greghaines.jesque.client.Client;
import net.greghaines.jesque.client.ClientPoolImpl;
import net.greghaines.jesque.worker.MapBasedJobFactory;
import net.greghaines.jesque.worker.WorkerImplFactory;
import net.greghaines.jesque.worker.WorkerPool;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisPool;

public class Issue56Test {

    private static final Logger LOG = LoggerFactory.getLogger(Issue56Test.class);
    private static final Config CONFIG = new ConfigBuilder().build();
    private static final Client CLIENT = new ClientPoolImpl(CONFIG, new JedisPool("localhost"));
    private static final String QUEUE = "default";

    @Test
    public void testZREM() {
        // Start workers
        final WorkerImplFactory workerFactory = new WorkerImplFactory(CONFIG, Arrays.asList(QUEUE), 
                new MapBasedJobFactory(map(entry(TestAction.class.getSimpleName(), TestAction.class))));
        final WorkerPool workerPool = new WorkerPool(workerFactory, 10);
        workerPool.run();

        // Start jobs
        enqueue();

        // Wait a few seconds then shutdown
        try { Thread.sleep(15000); } catch (Exception e){} // Give ourselves time to process
        CLIENT.end();
        try { workerPool.endAndJoin(true, 100); } catch (Exception e){ e.printStackTrace(); }
    }

    public static void enqueue() {
        final long future = System.currentTimeMillis() + 5;
        final Job job = new Job(TestAction.class.getSimpleName());
        CLIENT.delayedEnqueue(QUEUE, job, future);
    }

    public static class TestAction implements Runnable {
        
        private static final AtomicLong RUN_COUNT = new AtomicLong(0);
        
        /**
         * {@inheritDoc}
         */
        @Override
        public void run() {
            LOG.info("TestAction.run(): {}", RUN_COUNT.getAndIncrement());
            enqueue();
        }
    }
}
