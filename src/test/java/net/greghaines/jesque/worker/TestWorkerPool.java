package net.greghaines.jesque.worker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestWorkerPool {

    private static final int NUM_WORKERS = 2;
    
    private Mockery mockCtx;
    private Callable<? extends Worker> workerFactory;
    private final List<Worker> workers = new ArrayList<>(NUM_WORKERS);
    private final List<WorkerEventEmitter> eventEmitters = new ArrayList<>(NUM_WORKERS);
    private WorkerPool pool;
    
    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception {
        this.mockCtx = new JUnit4Mockery();
        this.mockCtx.setThreadingPolicy(new Synchroniser());
        this.workerFactory = this.mockCtx.mock(Callable.class);
        this.workers.add(this.mockCtx.mock(Worker.class, "Worker1"));
        this.workers.add(this.mockCtx.mock(Worker.class, "Worker2"));
        this.eventEmitters.add(this.mockCtx.mock(WorkerEventEmitter.class, "WorkerEventEmitter1"));
        this.eventEmitters.add(this.mockCtx.mock(WorkerEventEmitter.class, "WorkerEventEmitter2"));
        this.mockCtx.checking(new Expectations(){{
            oneOf(workerFactory).call(); will(returnValue(workers.get(0)));
            oneOf(workerFactory).call(); will(returnValue(workers.get(1)));
            allowing(workers.get(0)).getWorkerEventEmitter(); will(returnValue(eventEmitters.get(0)));
            allowing(workers.get(1)).getWorkerEventEmitter(); will(returnValue(eventEmitters.get(1)));
        }});
        this.pool = new WorkerPool(this.workerFactory, NUM_WORKERS);
    }
    
    @After
    public void tearDown() throws InterruptedException {
        this.mockCtx.checking(new Expectations(){{
           oneOf(workers.get(0)).end(true);
           oneOf(workers.get(1)).end(true);
        }});
        this.pool.endAndJoin(true, 1);
        this.mockCtx.assertIsSatisfied();
        this.workers.clear();
    }
    
    @Test
    public void testGetName() {
        this.mockCtx.checking(new Expectations(){{
            oneOf(workers.get(0)).getName(); will(returnValue("Worker1"));
            oneOf(workers.get(1)).getName(); will(returnValue("Worker2"));
        }});
        final String name = this.pool.getName();
        Assert.assertNotNull(name);
        Assert.assertTrue(name.contains("Worker1"));
        Assert.assertTrue(name.contains("Worker2"));
    }
    
    @Test
    public void testAddListener() {
        final WorkerListener listener = this.mockCtx.mock(WorkerListener.class);
        this.mockCtx.checking(new Expectations(){{
            oneOf(eventEmitters.get(0)).addListener(listener);
            oneOf(eventEmitters.get(1)).addListener(listener);
        }});
        this.pool.getWorkerEventEmitter().addListener(listener);
    }
    
    @Test
    public void testAddListener_Specific() {
        final WorkerListener listener = this.mockCtx.mock(WorkerListener.class);
        final WorkerEvent event = WorkerEvent.JOB_EXECUTE;
        final WorkerEvent event2 = WorkerEvent.JOB_FAILURE;
        this.mockCtx.checking(new Expectations(){{
            oneOf(eventEmitters.get(0)).addListener(listener, event, event2);
            oneOf(eventEmitters.get(1)).addListener(listener, event, event2);
        }});
        this.pool.getWorkerEventEmitter().addListener(listener, event, event2);
    }
    
    @Test
    public void testRemoveListener() {
        final WorkerListener listener = this.mockCtx.mock(WorkerListener.class);
        this.mockCtx.checking(new Expectations(){{
            oneOf(eventEmitters.get(0)).removeListener(listener);
            oneOf(eventEmitters.get(1)).removeListener(listener);
        }});
        this.pool.getWorkerEventEmitter().removeListener(listener);
    }
    
    @Test
    public void testRemoveListener_Specific() {
        final WorkerListener listener = this.mockCtx.mock(WorkerListener.class);
        final WorkerEvent event = WorkerEvent.JOB_EXECUTE;
        final WorkerEvent event2 = WorkerEvent.JOB_FAILURE;
        this.mockCtx.checking(new Expectations(){{
            oneOf(eventEmitters.get(0)).removeListener(listener, event, event2);
            oneOf(eventEmitters.get(1)).removeListener(listener, event, event2);
        }});
        this.pool.getWorkerEventEmitter().removeListener(listener, event, event2);
    }
    
    @Test
    public void testRemoveAllListeners() {
        this.mockCtx.checking(new Expectations(){{
            oneOf(eventEmitters.get(0)).removeAllListeners();
            oneOf(eventEmitters.get(1)).removeAllListeners();
        }});
        this.pool.getWorkerEventEmitter().removeAllListeners();
    }
    
    @Test
    public void testRemoveAllListeners_Specific() {
        final WorkerEvent event = WorkerEvent.JOB_EXECUTE;
        final WorkerEvent event2 = WorkerEvent.JOB_FAILURE;
        this.mockCtx.checking(new Expectations(){{
            oneOf(eventEmitters.get(0)).removeAllListeners(event, event2);
            oneOf(eventEmitters.get(1)).removeAllListeners(event, event2);
        }});
        this.pool.getWorkerEventEmitter().removeAllListeners(event, event2);
    }
    
    @Test
    public void testIsShutdown() {
        this.mockCtx.checking(new Expectations(){{
            oneOf(workers.get(0)).isShutdown(); will(returnValue(true));
        }});
        Assert.assertTrue(this.pool.isShutdown());
    }
    
    @Test
    public void testIsPaused() {
        this.mockCtx.checking(new Expectations(){{
            oneOf(workers.get(0)).isPaused(); will(returnValue(true));
        }});
        Assert.assertTrue(this.pool.isPaused());
    }
    
    @Test
    public void testTogglePause() {
        final boolean pause = true;
        this.mockCtx.checking(new Expectations(){{
            oneOf(workers.get(0)).togglePause(pause);
            oneOf(workers.get(1)).togglePause(pause);
        }});
        this.pool.togglePause(pause);
    }
    
    @Test
    public void testIsProcessingJob() {
        this.mockCtx.checking(new Expectations(){{
            oneOf(workers.get(0)).isProcessingJob(); will(returnValue(false));
            oneOf(workers.get(1)).isProcessingJob(); will(returnValue(true));
        }});
        Assert.assertTrue(this.pool.isProcessingJob());
        this.mockCtx.checking(new Expectations(){{
            oneOf(workers.get(0)).isProcessingJob(); will(returnValue(false));
            oneOf(workers.get(1)).isProcessingJob(); will(returnValue(false));
        }});
        Assert.assertFalse(this.pool.isProcessingJob());
    }
    
    @Test
    public void testGetQueues() {
        final Collection<String> queues = Arrays.asList("queue1", "queue2");
        this.mockCtx.checking(new Expectations(){{
            oneOf(workers.get(0)).getQueues(); will(returnValue(queues));
        }});
        Assert.assertEquals(queues, this.pool.getQueues());
    }
    
    @Test
    public void testAddQueue() {
        final String queueName = "queue1";
        this.mockCtx.checking(new Expectations(){{
            oneOf(workers.get(0)).addQueue(queueName);
            oneOf(workers.get(1)).addQueue(queueName);
        }});
        this.pool.addQueue(queueName);
    }
    
    @Test
    public void testRemoveQueue() {
        final String queueName = "queue1";
        final boolean all = true;
        this.mockCtx.checking(new Expectations(){{
            oneOf(workers.get(0)).removeQueue(queueName, all);
            oneOf(workers.get(1)).removeQueue(queueName, all);
        }});
        this.pool.removeQueue(queueName, all);
    }
    
    @Test
    public void testRemoveAllQueues() {
        this.mockCtx.checking(new Expectations(){{
            oneOf(workers.get(0)).removeAllQueues();
            oneOf(workers.get(1)).removeAllQueues();
        }});
        this.pool.removeAllQueues();
    }
    
    @Test
    public void testSetQueues() {
        final Collection<String> queues = Arrays.asList("queue1", "queue2");
        this.mockCtx.checking(new Expectations(){{
            oneOf(workers.get(0)).setQueues(queues);
            oneOf(workers.get(1)).setQueues(queues);
        }});
        this.pool.setQueues(queues);
    }

    @Test
    public void testGetJobFactory() {
        final Map<String, Class<?>> jobTypes = new LinkedHashMap<String, Class<?>>();
        final MapBasedJobFactory jobFactory = new MapBasedJobFactory(jobTypes);
        this.mockCtx.checking(new Expectations(){{
            oneOf(workers.get(0)).getJobFactory(); will(returnValue(jobFactory));
        }});
        Assert.assertEquals(jobFactory, this.pool.getJobFactory());
    }
    
    @Test
    public void testGetExceptionHandler() {
        final ExceptionHandler exceptionHandler = new DefaultExceptionHandler();
        this.mockCtx.checking(new Expectations(){{
            oneOf(workers.get(0)).getExceptionHandler(); will(returnValue(exceptionHandler));
        }});
        Assert.assertEquals(exceptionHandler, this.pool.getExceptionHandler());
    }
    
    @Test
    public void testSetExceptionHandler() {
        final ExceptionHandler exceptionHandler = new DefaultExceptionHandler();
        this.mockCtx.checking(new Expectations(){{
            oneOf(workers.get(0)).setExceptionHandler(exceptionHandler);
            oneOf(workers.get(1)).setExceptionHandler(exceptionHandler);
        }});
        this.pool.setExceptionHandler(exceptionHandler);
    }

    @Test
    public void testWorkerCounts() {
        this.mockCtx.checking(new Expectations(){{
            allowing(workers.get(0)).isProcessingJob();
            will(onConsecutiveCalls(returnValue(false), returnValue(false),
                    returnValue(false), returnValue(false),
                    returnValue(true), returnValue(true)));
            allowing(workers.get(1)).isProcessingJob();
            will(onConsecutiveCalls(returnValue(false), returnValue(false),
                    returnValue(true), returnValue(true),
                    returnValue(true), returnValue(true)));
        }});
        Assert.assertEquals(NUM_WORKERS, this.pool.getWorkerCount());
        Assert.assertEquals(0, this.pool.getActiveWorkerCount());
        Assert.assertEquals(NUM_WORKERS, this.pool.getIdleWorkerCount());
        Assert.assertEquals(NUM_WORKERS, this.pool.getWorkerCount());
        Assert.assertEquals(1, this.pool.getActiveWorkerCount());
        Assert.assertEquals(1, this.pool.getIdleWorkerCount());
        Assert.assertEquals(NUM_WORKERS, this.pool.getWorkerCount());
        Assert.assertEquals(NUM_WORKERS, this.pool.getActiveWorkerCount());
        Assert.assertEquals(0, this.pool.getIdleWorkerCount());
    }
}
