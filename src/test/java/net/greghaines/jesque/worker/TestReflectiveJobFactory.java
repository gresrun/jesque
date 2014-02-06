package net.greghaines.jesque.worker;

import java.util.concurrent.Callable;

import net.greghaines.jesque.Job;
import net.greghaines.jesque.utils.JesqueUtils;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests ReflectiveJobFactory.
 */
public class TestReflectiveJobFactory {

    @Test
    public void testMaterializeJob() throws Exception {
        final ReflectiveJobFactory jobFactory = new ReflectiveJobFactory();
        final Object action = jobFactory.materializeJob(new Job(TestRunnableJob.class.getName()));
        Assert.assertNotNull(action);
        Assert.assertEquals(TestRunnableJob.class, action.getClass());
        final Object action2 = jobFactory.materializeJob(new Job(TestCallableJob.class.getName()));
        Assert.assertNotNull(action2);
        Assert.assertEquals(TestCallableJob.class, action2.getClass());
    }
    
    @Test(expected = ClassCastException.class)
    public void testMaterializeJob_NotRunnable() throws Exception {
        JesqueUtils.materializeJob(new Job(TestBadJob.class.getName()));
    }
    
    public static class TestRunnableJob implements Runnable {
        @Override
        public void run() {
            // Do nothing
        }
    }
    
    public static class TestCallableJob implements Callable<Object> {
        @Override
        public Object call() {
            // Do nothing
            return null;
        }
    }
    
    public static class TestBadJob {
        public void run() {
            // Do nothing
        }
    }
}
