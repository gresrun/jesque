package net.greghaines.jesque.worker;

import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class TestWorkerPool {

  private static final int NUM_WORKERS = 2;

  @Mock private Callable<? extends Worker> workerFactory;

  private final List<Worker> workers = new ArrayList<>(NUM_WORKERS);
  private final List<WorkerEventEmitter> eventEmitters = new ArrayList<>(NUM_WORKERS);
  private WorkerPool pool;

  @SuppressWarnings("rawtypes")
  @Before
  public void setUp() throws Exception {
    this.workers.add(mock(Worker.class));
    this.workers.add(mock(Worker.class));
    this.eventEmitters.add(mock(WorkerEventEmitter.class));
    this.eventEmitters.add(mock(WorkerEventEmitter.class));
    when(((Callable) this.workerFactory).call())
        .thenReturn(this.workers.get(0), this.workers.get(1));
    when(this.workers.get(0).getWorkerEventEmitter()).thenReturn(this.eventEmitters.get(0));
    when(this.workers.get(1).getWorkerEventEmitter()).thenReturn(this.eventEmitters.get(1));
    this.pool = new WorkerPool(this.workerFactory, NUM_WORKERS);
  }

  @After
  public void tearDown() throws InterruptedException {
    this.pool.endAndJoin(true, 1);
    verify(this.workers.get(0)).end(true);
    verify(this.workers.get(1)).end(true);
    this.workers.clear();
  }

  @Test
  public void testGetName() {
    when(this.workers.get(0).getName()).thenReturn("Worker1");
    when(this.workers.get(1).getName()).thenReturn("Worker2");
    final String name = this.pool.getName();
    Assert.assertNotNull(name);
    Assert.assertTrue(name.contains("Worker1"));
    Assert.assertTrue(name.contains("Worker2"));
  }

  @Test
  public void testAddListener() {
    final WorkerListener listener = mock(WorkerListener.class);
    this.pool.getWorkerEventEmitter().addListener(listener);
    verify(this.eventEmitters.get(0)).addListener(listener);
    verify(this.eventEmitters.get(1)).addListener(listener);
  }

  @Test
  public void testAddListener_Specific() {
    final WorkerListener listener = mock(WorkerListener.class);
    final WorkerEvent event = WorkerEvent.JOB_EXECUTE;
    final WorkerEvent event2 = WorkerEvent.JOB_FAILURE;
    this.pool.getWorkerEventEmitter().addListener(listener, event, event2);
    verify(this.eventEmitters.get(0)).addListener(listener, event, event2);
    verify(this.eventEmitters.get(1)).addListener(listener, event, event2);
  }

  @Test
  public void testRemoveListener() {
    final WorkerListener listener = mock(WorkerListener.class);
    this.pool.getWorkerEventEmitter().removeListener(listener);
    verify(this.eventEmitters.get(0)).removeListener(listener);
    verify(this.eventEmitters.get(1)).removeListener(listener);
  }

  @Test
  public void testRemoveListener_Specific() {
    final WorkerListener listener = mock(WorkerListener.class);
    final WorkerEvent event = WorkerEvent.JOB_EXECUTE;
    final WorkerEvent event2 = WorkerEvent.JOB_FAILURE;
    this.pool.getWorkerEventEmitter().removeListener(listener, event, event2);
    verify(this.eventEmitters.get(0)).removeListener(listener, event, event2);
    verify(this.eventEmitters.get(1)).removeListener(listener, event, event2);
  }

  @Test
  public void testRemoveAllListeners() {
    this.pool.getWorkerEventEmitter().removeAllListeners();
    verify(this.eventEmitters.get(0)).removeAllListeners();
    verify(this.eventEmitters.get(1)).removeAllListeners();
  }

  @Test
  public void testRemoveAllListeners_Specific() {
    final WorkerEvent event = WorkerEvent.JOB_EXECUTE;
    final WorkerEvent event2 = WorkerEvent.JOB_FAILURE;
    this.pool.getWorkerEventEmitter().removeAllListeners(event, event2);
    verify(this.eventEmitters.get(0)).removeAllListeners(event, event2);
    verify(this.eventEmitters.get(1)).removeAllListeners(event, event2);
  }

  @Test
  public void testIsShutdown() {
    when(this.workers.get(0).isShutdown()).thenReturn(true);
    Assert.assertTrue(this.pool.isShutdown());
  }

  @Test
  public void testIsPaused() {
    when(this.workers.get(0).isPaused()).thenReturn(true);
    Assert.assertTrue(this.pool.isPaused());
  }

  @Test
  public void testTogglePause() {
    final boolean pause = true;
    this.pool.togglePause(pause);
    verify(this.workers.get(0)).togglePause(pause);
    verify(this.workers.get(1)).togglePause(pause);
  }

  @Test
  public void testIsProcessingJob() {
    when(this.workers.get(0).isProcessingJob()).thenReturn(false);
    when(this.workers.get(1).isProcessingJob()).thenReturn(true);
    Assert.assertTrue(this.pool.isProcessingJob());
    when(this.workers.get(0).isProcessingJob()).thenReturn(false);
    when(this.workers.get(1).isProcessingJob()).thenReturn(false);
    Assert.assertFalse(this.pool.isProcessingJob());
  }

  @Test
  public void testGetQueues() {
    final Collection<String> queues = Arrays.asList("queue1", "queue2");
    when(this.workers.get(0).getQueues()).thenReturn(queues);
    Assert.assertEquals(queues, this.pool.getQueues());
  }

  @Test
  public void testAddQueue() {
    final String queueName = "queue1";
    this.pool.addQueue(queueName);
    verify(this.workers.get(0)).addQueue(queueName);
    verify(this.workers.get(1)).addQueue(queueName);
  }

  @Test
  public void testRemoveQueue() {
    final String queueName = "queue1";
    final boolean all = true;
    this.pool.removeQueue(queueName, all);
    verify(this.workers.get(0)).removeQueue(queueName, all);
    verify(this.workers.get(1)).removeQueue(queueName, all);
  }

  @Test
  public void testRemoveAllQueues() {
    this.pool.removeAllQueues();
    verify(this.workers.get(0)).removeAllQueues();
    verify(this.workers.get(1)).removeAllQueues();
  }

  @Test
  public void testSetQueues() {
    final Collection<String> queues = Arrays.asList("queue1", "queue2");
    this.pool.setQueues(queues);
    verify(this.workers.get(0)).setQueues(queues);
    verify(this.workers.get(1)).setQueues(queues);
  }

  @Test
  public void testGetJobFactory() {
    final Map<String, Class<?>> jobTypes = new LinkedHashMap<String, Class<?>>();
    final MapBasedJobFactory jobFactory = new MapBasedJobFactory(jobTypes);
    when(this.workers.get(0).getJobFactory()).thenReturn(jobFactory);
    Assert.assertEquals(jobFactory, this.pool.getJobFactory());
  }

  @Test
  public void testGetExceptionHandler() {
    final ExceptionHandler exceptionHandler = new DefaultExceptionHandler();
    when(this.workers.get(0).getExceptionHandler()).thenReturn(exceptionHandler);
    Assert.assertEquals(exceptionHandler, this.pool.getExceptionHandler());
  }

  @Test
  public void testSetExceptionHandler() {
    final ExceptionHandler exceptionHandler = new DefaultExceptionHandler();
    this.pool.setExceptionHandler(exceptionHandler);
    verify(this.workers.get(0)).setExceptionHandler(exceptionHandler);
    verify(this.workers.get(1)).setExceptionHandler(exceptionHandler);
  }

  @Test
  public void testWorkerCounts() {
    when(this.workers.get(0).isProcessingJob()).thenReturn(false, false, false, false, true, true);
    when(this.workers.get(1).isProcessingJob()).thenReturn(false, false, true, true, true, true);
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
