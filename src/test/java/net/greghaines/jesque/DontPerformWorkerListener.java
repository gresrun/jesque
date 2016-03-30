package net.greghaines.jesque;

import net.greghaines.jesque.worker.DontPerformException;
import net.greghaines.jesque.worker.Worker;
import net.greghaines.jesque.worker.WorkerEvent;
import net.greghaines.jesque.worker.WorkerListener;

/**
 * {@link WorkerListener} that throws a {@link DontPerformException} on a given event.
 */
public class DontPerformWorkerListener implements WorkerListener {
    /**
     * Event to answer with a {@link DontPerformException}.
     */
    private final WorkerEvent event;

    /**
     * Constructor.
     *
     * @param event Event to answer with a {@link DontPerformException}.
     */
    public DontPerformWorkerListener(WorkerEvent event) {
        this.event = event;
    }

    @Override
    public void onEvent(WorkerEvent event, Worker worker, String queue, Job job, Object runner, Object result, Throwable t) {
        if (event.equals(this.event)) {
            throw new DontPerformException(event.name());
        }
    }
}
