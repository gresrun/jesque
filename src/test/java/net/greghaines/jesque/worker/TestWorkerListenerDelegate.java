package net.greghaines.jesque.worker;

import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class TestWorkerListenerDelegate {

  @Mock private WorkerListener listener;
  private WorkerListenerDelegate delegate;

  @Before
  public void setUp() {
    this.delegate = new WorkerListenerDelegate();
  }

  @Test
  public void testAddListener() {
    final WorkerEvent event = WorkerEvent.JOB_EXECUTE;
    this.delegate.addListener(this.listener);
    this.delegate.fireEvent(event, null, null, null, null, null, null);
    verify(this.listener)
        .onEvent(eq(event), isNull(), isNull(), isNull(), isNull(), isNull(), isNull());
    this.delegate.removeListener(this.listener);
    this.delegate.fireEvent(event, null, null, null, null, null, null);
    verifyNoMoreInteractions(this.listener);
  }

  @Test
  public void testAddListener_Specific() {
    final WorkerEvent event = WorkerEvent.JOB_EXECUTE;
    final WorkerEvent event2 = WorkerEvent.JOB_FAILURE;
    this.delegate.addListener(this.listener, event);
    this.delegate.fireEvent(event, null, null, null, null, null, null);
    verify(this.listener)
        .onEvent(eq(event), isNull(), isNull(), isNull(), isNull(), isNull(), isNull());
    this.delegate.fireEvent(event2, null, null, null, null, null, null);
    this.delegate.removeListener(this.listener, event);
    this.delegate.fireEvent(event, null, null, null, null, null, null);
    this.delegate.fireEvent(event2, null, null, null, null, null, null);
    verifyNoMoreInteractions(this.listener);
  }

  @Test
  public void testRemoveListener_Specific() {
    final WorkerEvent event = WorkerEvent.JOB_EXECUTE;
    final WorkerEvent event2 = WorkerEvent.JOB_FAILURE;
    this.delegate.addListener(this.listener, event, event2);
    this.delegate.fireEvent(event, null, null, null, null, null, null);
    this.delegate.fireEvent(event2, null, null, null, null, null, null);
    this.delegate.removeListener(this.listener, event2);
    this.delegate.fireEvent(event, null, null, null, null, null, null);
    this.delegate.fireEvent(event2, null, null, null, null, null, null);
    verify(this.listener, times(2))
        .onEvent(eq(event), isNull(), isNull(), isNull(), isNull(), isNull(), isNull());
    verify(this.listener)
        .onEvent(eq(event2), isNull(), isNull(), isNull(), isNull(), isNull(), isNull());
    verifyNoMoreInteractions(this.listener);
  }

  @Test
  public void testRemoveAllListeners() {
    final WorkerEvent event = WorkerEvent.JOB_EXECUTE;
    this.delegate.addListener(this.listener);
    this.delegate.fireEvent(event, null, null, null, null, null, null);
    verify(this.listener)
        .onEvent(eq(event), isNull(), isNull(), isNull(), isNull(), isNull(), isNull());
    this.delegate.removeAllListeners();
    this.delegate.fireEvent(event, null, null, null, null, null, null);
    verifyNoMoreInteractions(this.listener);
  }

  @Test
  public void testRemoveAllListeners_Specific() {
    final WorkerEvent event = WorkerEvent.JOB_EXECUTE;
    final WorkerEvent event2 = WorkerEvent.JOB_FAILURE;
    this.delegate.addListener(this.listener, event, event2);
    this.delegate.fireEvent(event, null, null, null, null, null, null);
    this.delegate.fireEvent(event2, null, null, null, null, null, null);
    this.delegate.removeAllListeners(event2);
    this.delegate.fireEvent(event, null, null, null, null, null, null);
    this.delegate.fireEvent(event2, null, null, null, null, null, null);
    verify(this.listener, times(2))
        .onEvent(eq(event), isNull(), isNull(), isNull(), isNull(), isNull(), isNull());
    verify(this.listener)
        .onEvent(eq(event2), isNull(), isNull(), isNull(), isNull(), isNull(), isNull());
    verifyNoMoreInteractions(this.listener);
  }
}
