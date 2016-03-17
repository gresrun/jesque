package net.greghaines.jesque;

import net.greghaines.jesque.utils.Sleep;
import net.greghaines.jesque.worker.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import static net.greghaines.jesque.utils.JesqueUtils.entry;
import static net.greghaines.jesque.utils.JesqueUtils.map;

public class LongRunningTest {
    
    private static final Logger log = LoggerFactory.getLogger(LongRunningTest.class);
    private static final Config config = new ConfigBuilder().build();

    private static void changeRedisTimeout(final long seconds) {
        final Jedis jedis = TestUtils.createJedis(config);
        jedis.configSet("timeout", Long.toString(seconds));
        jedis.disconnect();
    }

    @Before
    public void resetRedis() throws Exception {
        TestUtils.resetRedis(config);
    }

    @Test
    @Ignore
    public void issue28() throws InterruptedException {
        changeRedisTimeout(10);
        TestUtils.enqueueJobs("longRunning", Arrays.asList(new Job("LongRunningAction", 20 * 1000L)), config);
        final Worker worker2 = new WorkerImpl(config, Arrays.asList("longRunning"), 
                new MapBasedJobFactory(map(entry("LongRunningAction", LongRunningAction.class))));
        final AtomicBoolean successRef = new AtomicBoolean(false);
        worker2.getWorkerEventEmitter().addListener(new WorkerListener() {
            @Override
            public void onEvent(final WorkerEvent event, final Worker worker, final String queue, final Job job,
                    final Object runner, final Object result, final Throwable t) {
                successRef.set(true);
                log.info("SUCCCESS: {}", job);
            }
        }, WorkerEvent.JOB_SUCCESS);
        final Thread workerThread2 = new Thread(worker2);
        workerThread2.start();
        Sleep.sleep(1000);
        worker2.end(false);
        workerThread2.join();

        Assert.assertTrue("Success callback should have been called", successRef.get());
        changeRedisTimeout(0);
    }

    public static class LongRunningAction implements Runnable {
        
        private final int millis;

        public LongRunningAction(final int millis) {
            this.millis = millis;
        }

        public void run() {
            Sleep.sleep(this.millis);
        }
    }
}
