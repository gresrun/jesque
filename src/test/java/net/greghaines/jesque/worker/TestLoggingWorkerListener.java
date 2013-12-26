package net.greghaines.jesque.worker;

import org.junit.Test;

public class TestLoggingWorkerListener {

    @Test
    public void testLoggingWorkerListener() {
        final LoggingWorkerListener listener = LoggingWorkerListener.INSTANCE;
        listener.onEvent(WorkerEvent.JOB_EXECUTE, null, null, null, null, null, null);
        listener.onEvent(WorkerEvent.JOB_EXECUTE, null, null, null, null, null, new Exception());
    }
}
