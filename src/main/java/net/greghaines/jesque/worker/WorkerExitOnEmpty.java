package net.greghaines.jesque.worker;

import static net.greghaines.jesque.utils.ResqueConstants.QUEUE;
import static net.greghaines.jesque.worker.JobExecutor.State.RUNNING;
import static net.greghaines.jesque.worker.WorkerEvent.WORKER_POLL;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.json.ObjectMapperFactory;
import net.greghaines.jesque.utils.JesqueUtils;

/**
 * An implementation of Worker that exits if all queues are empty
 * maxLoopOnEmptyQueues times.
 * <p/>
 * This is useful if you have more queues that you want active workers for at
 * one time, and you don't want workers working multiple queues.
 * <p/>
 * Essentially once a worker starts a queue you want that queue worked to
 * exhaustion, then you want that worker to die, and to start up another worker
 * on a new queue.
 * 
 * @author Timothy Hruska
 */
public class WorkerExitOnEmpty extends WorkerImpl {

    public static final int DEFAULT_MAX_LOOPS_ON_EMPTY_QUEUES = 3;

    private final int maxLoopsOnEmptyQueues;

    public WorkerExitOnEmpty(final Config config, final Collection<String> queues,
            final Map<String, ? extends Class<?>> jobTypes) {
        this(config, queues, jobTypes, DEFAULT_MAX_LOOPS_ON_EMPTY_QUEUES);
    }

    public WorkerExitOnEmpty(final Config config, final Collection<String> queues,
            final Map<String, ? extends Class<?>> jobTypes, final int maxLoopsOnEmptyQueues) {
        super(config, queues, jobTypes);
        this.maxLoopsOnEmptyQueues = maxLoopsOnEmptyQueues;
    }

    /**
     * Polls the queues for jobs and executes them.<br/>
     * Exits if all queues are empty maxLoopOnEmptyQueues times
     * 
     * @see net.greghaines.jesque.worker.WorkerImpl#poll()
     */
    @Override
    protected void poll() {
        int missCount = 0;
        String curQueue = null;
        int allQueuesEmptyCount = 0;
        while (RUNNING.equals(this.state.get())) {
            try {
                if (isThreadNameChangingEnabled()) {
                    renameThread("Waiting for " + JesqueUtils.join(",", this.queueNames));
                }
                curQueue = this.queueNames.poll(emptyQueueSleepTime, TimeUnit.MILLISECONDS);
                if (curQueue != null) {
                    this.queueNames.add(curQueue); // Rotate the queues
                    checkPaused();
                    // Might have been waiting in poll()/checkPaused() for a
                    // while
                    if (RUNNING.equals(this.state.get())) {
                        this.listenerDelegate.fireEvent(WORKER_POLL, this, curQueue, null, null, null, null);
                        final String payload = this.jedis.lpop(key(QUEUE, curQueue));
                        if (payload != null) {
                            final Job job = ObjectMapperFactory.get().readValue(payload, Job.class);
                            process(job, curQueue);
                            missCount = 0;
                            allQueuesEmptyCount = 0;
                        } else if ((++missCount >= this.queueNames.size()) && RUNNING.equals(this.state.get())) {
                            // Keeps worker from busy-spinning on empty queues
                            missCount = 0;
                            Thread.sleep(emptyQueueSleepTime);
                            allQueuesEmptyCount++;
                        }
                        if (allQueuesEmptyCount >= this.maxLoopsOnEmptyQueues) {
                            // Set state to SHUTDOWN to break the loop
                            end(false);
                        }
                    }
                }
            } catch (Exception e) {
                recoverFromException(curQueue, e);
            }
        }
    }
}
