package net.greghaines.jesque.worker;

import static net.greghaines.jesque.utils.ResqueConstants.QUEUE;
import static net.greghaines.jesque.worker.WorkerEvent.WORKER_POLL;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.json.ObjectMapperFactory;
import net.greghaines.jesque.utils.JesqueUtils;

public class WorkerExitOnEmpty extends WorkerImpl {
    private int maxLoopsOnEmptyQueues;

    public WorkerExitOnEmpty(Config config, Collection<String> queues, Map<String, ? extends Class<?>> jobTypes) {
        this(config, queues, jobTypes, 3);
    }

    public WorkerExitOnEmpty(Config config, Collection<String> queues, Map<String, ? extends Class<?>> jobTypes,
        int maxLoopsOnEmptyQueues) {
        super(config, queues, jobTypes);
        this.maxLoopsOnEmptyQueues = maxLoopsOnEmptyQueues;
    }

    /* (non-Javadoc)
     * @see net.greghaines.jesque.worker.WorkerImpl#poll()
     * Exists if all queues are empty maxLoopOnEmptyQueues times
     */
    @Override
    protected void poll() {
        int missCount = 0;
        String curQueue = null;
        int allQueuesEmptyCount = 0;

        while (WorkerState.RUNNING.equals(this.state.get())) {
            try {
                renameThread("Waiting for " + JesqueUtils.join(",", this.queueNames));
                curQueue = this.queueNames.poll(emptyQueueSleepTime, TimeUnit.MILLISECONDS);
                if (curQueue != null) {
                    this.queueNames.add(curQueue); // Rotate the queues
                    checkPaused();
                    if (WorkerState.RUNNING.equals(this.state.get())) // Might have been waiting in poll()/checkPaused() for a while
                    {
                        this.listenerDelegate.fireEvent(WORKER_POLL, this, curQueue, null, null, null, null);
                        final String payload = this.jedis.lpop(key(QUEUE, curQueue));
                        if (payload != null) {
                            final Job job = ObjectMapperFactory.get().readValue(payload, Job.class);
                            process(job, curQueue);
                            missCount = 0;
                            allQueuesEmptyCount = 0;
                        } else if ((++missCount >= this.queueNames.size()) && WorkerState.RUNNING.equals(this.state.get())) { // Keeps worker from busy-spinning on empty queues
                            missCount = 0;
                            Thread.sleep(emptyQueueSleepTime);
                            allQueuesEmptyCount++;
                        }

                        if (allQueuesEmptyCount >= maxLoopsOnEmptyQueues) {
                            end(false); // sets state to SHUTDOWN which will break the loop
                        }
                    }
                }
            } catch (Exception e) {
                recoverFromException(curQueue, e);
            }
        }
    }
}
