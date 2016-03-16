/*
 * Copyright 2011 Greg Haines
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.greghaines.jesque.client;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.json.ObjectMapperFactory;
import net.greghaines.jesque.queue.LockDao;
import net.greghaines.jesque.queue.QueueDao;
import net.greghaines.jesque.utils.JesqueUtils;

/**
 * Common logic for Client implementations.
 * 
 * @author Greg Haines
 * @author Animesh Kumar
 */
public abstract class AbstractClient implements Client {

    protected final Config config;
    protected final QueueDao queueDao;
    protected final LockDao lockDao;

    /**
     * Constructor.
     * 
     * @param config
     *            used to get the namespace for key creation
     */
    protected AbstractClient(final Config config, final QueueDao queueDao, final LockDao lockDao) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        if (queueDao == null) {
            throw new IllegalArgumentException("queueDao must not be null");
        }
        if (lockDao == null) {
            throw new IllegalArgumentException("lockDao must not be null");
        }
        this.config = config;
        this.queueDao = queueDao;
        this.lockDao = lockDao;
    }

    /**
     * @return the namespace this client will use
     */
    protected String getNamespace() {
        return this.config.getNamespace();
    }

    /**
     * Builds a namespaced Redis key with the given arguments.
     * 
     * @param parts
     *            the key parts to be joined
     * @return an assembled String key
     */
    protected String key(final String... parts) {
        return JesqueUtils.createKey(getNamespace(), parts);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void enqueue(final String queue, final Job job) {
        validateArguments(queue, job);
        try {
            queueDao.enqueue(queue, ObjectMapperFactory.get().writeValueAsString(job));
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void priorityEnqueue(final String queue, final Job job) {
        validateArguments(queue, job);
        try {
            queueDao.priorityEnqueue(queue, ObjectMapperFactory.get().writeValueAsString(job));
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean acquireLock(final String lockName, final String lockHolder, final int timeout) {
        if ((lockName == null) || "".equals(lockName)) {
            throw new IllegalArgumentException("lockName must not be null or empty: " + lockName);
        }
        if ((lockHolder == null) || "".equals(lockHolder)) {
            throw new IllegalArgumentException("lockHolder must not be null or empty: " + lockHolder);
        }
        if (timeout < 1) {
            throw new IllegalArgumentException("timeout must be a positive number");
        }
        try {
            return lockDao.acquireLock(lockName, lockHolder, timeout);
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delayedEnqueue(final String queue, final Job job, final long future) {
        validateArguments(queue, job, future);
        try {
            queueDao.delayedEnqueue(queue, ObjectMapperFactory.get().writeValueAsString(job), future);
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeDelayedEnqueue(final String queue, final Job job) {
        validateArguments(queue, job);
        try {
            queueDao.removeDelayedEnqueue(queue, ObjectMapperFactory.get().writeValueAsString(job));
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recurringEnqueue(String queue, Job job, long future, long frequency) {
        validateArguments(queue, job, future, frequency);
        try {
            queueDao.recurringEnqueue(queue, ObjectMapperFactory.get().writeValueAsString(job), future, frequency);
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void removeRecurringEnqueue(String queue, Job job) {
        validateArguments(queue, job);
        try {
            queueDao.removeRecurringEnqueue(queue, ObjectMapperFactory.get().writeValueAsString(job));
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void end() {
        // Overwrite, if needed.
    }

    private void validateArguments(final String queue, final Job job) {
        if (queue == null || "".equals(queue)) {
            throw new IllegalArgumentException("queue must not be null or empty: " + queue);
        }
        if (job == null) {
            throw new IllegalArgumentException("job must not be null");
        }
        if (!job.isValid()) {
            throw new IllegalStateException("job is not valid: " + job);
        }
    }

    private void validateArguments(final String queue, final Job job, final long future) {
        validateArguments(queue, job);
        if (System.currentTimeMillis() > future) {
            throw new IllegalArgumentException("future must be after current time");
        }
    }

    private void validateArguments(final String queue, final Job job, final long future, final long frequency) {
        validateArguments(queue, job, future);
        if (frequency < 1) {
            throw new IllegalArgumentException("frequency must be greater than one second");
        }
    }
}
