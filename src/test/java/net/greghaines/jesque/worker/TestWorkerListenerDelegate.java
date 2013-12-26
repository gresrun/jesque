package net.greghaines.jesque.worker;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.junit.Before;
import org.junit.Test;

public class TestWorkerListenerDelegate {
    
    private WorkerListenerDelegate delegate;
    private Mockery mockCtx;
    private WorkerListener listener;

    @Before
    public void setUp() {
        this.delegate = new WorkerListenerDelegate();
        this.mockCtx = new JUnit4Mockery();
        this.listener = this.mockCtx.mock(WorkerListener.class);
    }
    
    @Test
    public void testAddListener() {
        final WorkerEvent event = WorkerEvent.JOB_EXECUTE;
        this.mockCtx.checking(new Expectations(){{
            oneOf(listener).onEvent(event, null, null, null, null, null, null);
        }});
        this.delegate.addListener(this.listener);
        this.delegate.fireEvent(event, null, null, null, null, null, null);
        this.delegate.removeListener(this.listener);
        this.delegate.fireEvent(event, null, null, null, null, null, null);
        this.mockCtx.assertIsSatisfied();
    }
    
    @Test
    public void testAddListener_Specific() {
        final WorkerEvent event = WorkerEvent.JOB_EXECUTE;
        final WorkerEvent event2 = WorkerEvent.JOB_FAILURE;
        this.mockCtx.checking(new Expectations(){{
            oneOf(listener).onEvent(event, null, null, null, null, null, null);
        }});
        this.delegate.addListener(this.listener, event);
        this.delegate.fireEvent(event, null, null, null, null, null, null);
        this.delegate.fireEvent(event2, null, null, null, null, null, null);
        this.delegate.removeListener(this.listener, event);
        this.delegate.fireEvent(event, null, null, null, null, null, null);
        this.delegate.fireEvent(event2, null, null, null, null, null, null);
        this.mockCtx.assertIsSatisfied();
    }
    
    @Test
    public void testRemoveListener_Specific() {
        final WorkerEvent event = WorkerEvent.JOB_EXECUTE;
        final WorkerEvent event2 = WorkerEvent.JOB_FAILURE;
        this.mockCtx.checking(new Expectations(){{
            exactly(2).of(listener).onEvent(event, null, null, null, null, null, null);
            oneOf(listener).onEvent(event2, null, null, null, null, null, null);
        }});
        this.delegate.addListener(this.listener, event, event2);
        this.delegate.fireEvent(event, null, null, null, null, null, null);
        this.delegate.fireEvent(event2, null, null, null, null, null, null);
        this.delegate.removeListener(this.listener, event2);
        this.delegate.fireEvent(event, null, null, null, null, null, null);
        this.delegate.fireEvent(event2, null, null, null, null, null, null);
        this.mockCtx.assertIsSatisfied();
    }
    
    @Test
    public void testRemoveAllListeners() {
        final WorkerEvent event = WorkerEvent.JOB_EXECUTE;
        this.mockCtx.checking(new Expectations(){{
            oneOf(listener).onEvent(event, null, null, null, null, null, null);
        }});
        this.delegate.addListener(this.listener);
        this.delegate.fireEvent(event, null, null, null, null, null, null);
        this.delegate.removeAllListeners();
        this.delegate.fireEvent(event, null, null, null, null, null, null);
        this.mockCtx.assertIsSatisfied();
    }
    
    @Test
    public void testRemoveAllListeners_Specific() {
        final WorkerEvent event = WorkerEvent.JOB_EXECUTE;
        final WorkerEvent event2 = WorkerEvent.JOB_FAILURE;
        this.mockCtx.checking(new Expectations(){{
            exactly(2).of(listener).onEvent(event, null, null, null, null, null, null);
            oneOf(listener).onEvent(event2, null, null, null, null, null, null);
        }});
        this.delegate.addListener(this.listener, event, event2);
        this.delegate.fireEvent(event, null, null, null, null, null, null);
        this.delegate.fireEvent(event2, null, null, null, null, null, null);
        this.delegate.removeAllListeners(event2);
        this.delegate.fireEvent(event, null, null, null, null, null, null);
        this.delegate.fireEvent(event2, null, null, null, null, null, null);
        this.mockCtx.assertIsSatisfied();
    }
}
