package net.greghaines.jesque;

import static net.greghaines.jesque.utils.JesqueUtils.entry;
import static net.greghaines.jesque.utils.JesqueUtils.map;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.greghaines.jesque.worker.MapBasedJobFactory;
import net.greghaines.jesque.worker.Worker;
import net.greghaines.jesque.worker.WorkerImpl;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InfiniteTest {
    
    private static final Logger log = LoggerFactory.getLogger(InfiniteTest.class);
    private static final Config config = new ConfigBuilder().build();

    @Before
    public void resetRedis() throws Exception {
        TestUtils.resetRedis(config);
    }

    @Test
    @Ignore
    public void dontStopNow() throws InterruptedException {
        for (int i = 0; i < 5; i++) {
            final List<Job> jobs = new ArrayList<Job>(30);
            for (int j = 0; j < 30; j++) {
                jobs.add(new Job("TestAction", new Object[] { j, 2.3, true, "test", Arrays.asList("inner", 4.5) }));
            }
            TestUtils.enqueueJobs("foo" + i, jobs, config);
            jobs.clear();
            for (int j = 0; j < 5; j++) {
                jobs.add(new Job("FailAction"));
            }
            TestUtils.enqueueJobs("bar", jobs, config);
        }
        final Worker worker = new WorkerImpl(config, Arrays.asList("foo0", "bar", "baz"), 
                new MapBasedJobFactory(map(entry("TestAction", TestAction.class), 
                        entry("FailAction", FailAction.class))));
        final Thread workerThread = new Thread(worker);
        workerThread.start();

        TestUtils.enqueueJobs("inf", Arrays.asList(new Job("InfiniteAction")), config);
        final Worker worker2 = new WorkerImpl(config, Arrays.asList("inf"), 
                new MapBasedJobFactory(map(entry("InfiniteAction", InfiniteAction.class))));
        final Thread workerThread2 = new Thread(worker2);
        workerThread2.start();
        worker2.togglePause(true);

        workerThread.join();
    }

    @Test
    @Ignore
    public void issue6() throws InterruptedException {
        final Worker worker = new WorkerImpl(config, Arrays.asList("foo"), 
                new MapBasedJobFactory(map(entry("TestAction", TestAction.class),
                        entry("FailAction", FailAction.class))));
        final Thread workerThread = new Thread(worker);
        workerThread.start();

        for (int i = 0; i < 10; i++) {
            final List<Job> jobs = new ArrayList<Job>(30);
            for (int j = 0; j < 30; j++) {
                jobs.add(new Job("TestAction", new Object[] { j, 2.3, true, "test", Arrays.asList("inner", 4.5) }));
            }
            TestUtils.enqueueJobs("foo", jobs, config);
            Thread.sleep(1000);
        }
        log.info("DO IT NOW!!!");
        workerThread.join();
    }
}
