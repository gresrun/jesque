package net.greghaines.jesque;

import net.greghaines.jesque.worker.DontPerformException;
import net.greghaines.jesque.worker.Worker;
import net.greghaines.jesque.worker.WorkerEvent;
import net.greghaines.jesque.worker.WorkerListener;

/**
 * {@link WorkerListener} that throws a {@link DontPerformException} on {@link WorkerEvent#JOB_PROCESS}.
 */
public class DontPerformWorkerListener implements WorkerListener {
    @Override
    public void onEvent(WorkerEvent event, Worker worker, String queue, Job job, Object runner, Object result, Throwable t) {
        if (WorkerEvent.JOB_PROCESS.equals(event)) {
            throw new DontPerformException("test");
        }
    }
}
