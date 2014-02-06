package net.greghaines.jesque.worker;

import net.greghaines.jesque.Job;
import net.greghaines.jesque.utils.JesqueUtils;

/**
 * ReflectiveJobFactory assumes job names are fully-qualified class names.
 */
public class ReflectiveJobFactory implements JobFactory {

    /**
     * {@inheritDoc}
     */
    @Override
    public Object materializeJob(final Job job) throws Exception {
        return JesqueUtils.materializeJob(job);
    }
}
