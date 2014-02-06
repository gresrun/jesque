package net.greghaines.jesque.worker;

import net.greghaines.jesque.Job;

/**
 * JobFactory materializes jobs.
 */
public interface JobFactory {

    /**
     * Materializes a job.
     * @param job the job to materialize
     * @return the materialized job
     * @throws Exception if there was an exception creating the object
     */
    Object materializeJob(Job job) throws Exception;
}
