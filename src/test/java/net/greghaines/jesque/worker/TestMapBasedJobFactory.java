package net.greghaines.jesque.worker;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import net.greghaines.jesque.Job;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests MapBasedJobFactory.
 */
public class TestMapBasedJobFactory {
    
    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_Null() {
        new MapBasedJobFactory(null);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_NullName() throws Exception {
        final Map<String, Class<?>> jobTypes = new HashMap<String, Class<?>>();
        jobTypes.put(null, TestCallableJob.class);
        new MapBasedJobFactory(jobTypes);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_NullType() throws Exception {
        final Map<String, Class<?>> jobTypes = new HashMap<String, Class<?>>();
        jobTypes.put("TestCallableJob", null);
        new MapBasedJobFactory(jobTypes);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_NotRunnable() throws Exception {
        final Map<String, Class<?>> jobTypes = new HashMap<String, Class<?>>();
        jobTypes.put("TestBadJob", TestBadJob.class);
        new MapBasedJobFactory(jobTypes);
    }
    
    @Test
    public void testGetJobTypes() {
        final Map<String, Class<?>> jobTypes = new HashMap<String, Class<?>>();
        jobTypes.put("TestRunnableJob", TestRunnableJob.class);
        jobTypes.put("TestCallableJob", TestCallableJob.class);
        final MapBasedJobFactory jobFactory = new MapBasedJobFactory(jobTypes);
        Assert.assertEquals(jobTypes, jobFactory.getJobTypes());
    }
    
    @Test
    public void testsSetJobTypes() {
        final Map<String, Class<?>> jobTypes = new HashMap<String, Class<?>>();
        jobTypes.put("TestRunnableJob", TestRunnableJob.class);
        jobTypes.put("TestCallableJob", TestCallableJob.class);
        final MapBasedJobFactory jobFactory = new MapBasedJobFactory(new HashMap<String, Class<?>>());
        Assert.assertTrue(jobFactory.getJobTypes().isEmpty());
        jobFactory.setJobTypes(jobTypes);
        Assert.assertEquals(jobTypes, jobFactory.getJobTypes());
    }
    
    @Test
    public void testAddJobType() {
        final Map<String, Class<?>> jobTypes = new HashMap<String, Class<?>>();
        jobTypes.put("TestRunnableJob", TestRunnableJob.class);
        final MapBasedJobFactory jobFactory = new MapBasedJobFactory(jobTypes);
        Assert.assertEquals(jobTypes, jobFactory.getJobTypes());
        jobFactory.addJobType("TestCallableJob", TestCallableJob.class);
        jobTypes.put("TestCallableJob", TestCallableJob.class);
        Assert.assertEquals(jobTypes, jobFactory.getJobTypes());
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testAddJobType_NullName() {
        final MapBasedJobFactory jobFactory = new MapBasedJobFactory(new HashMap<String, Class<?>>());
        jobFactory.addJobType(null, TestCallableJob.class);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testAddJobType_NullType() {
        final MapBasedJobFactory jobFactory = new MapBasedJobFactory(new HashMap<String, Class<?>>());
        jobFactory.addJobType("TestCallableJob", null);
    }
    
    @Test
    public void testRemoveJobName() {
        final Map<String, Class<?>> jobTypes = new HashMap<String, Class<?>>();
        jobTypes.put("TestRunnableJob", TestRunnableJob.class);
        jobTypes.put("TestCallableJob", TestCallableJob.class);
        final MapBasedJobFactory jobFactory = new MapBasedJobFactory(jobTypes);
        Assert.assertEquals(jobTypes, jobFactory.getJobTypes());
        jobFactory.removeJobName("TestCallableJob");
        jobTypes.remove("TestCallableJob");
        Assert.assertEquals(jobTypes, jobFactory.getJobTypes());
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testRemoveJobName_Null() {
        final Map<String, Class<?>> jobTypes = new HashMap<String, Class<?>>();
        jobTypes.put("TestRunnableJob", TestRunnableJob.class);
        jobTypes.put("TestCallableJob", TestCallableJob.class);
        final MapBasedJobFactory jobFactory = new MapBasedJobFactory(jobTypes);
        Assert.assertEquals(jobTypes, jobFactory.getJobTypes());
        jobFactory.removeJobName(null);
    }
    
    @Test
    public void testRemoveJobType() {
        final Map<String, Class<?>> jobTypes = new HashMap<String, Class<?>>();
        jobTypes.put("TestRunnableJob", TestRunnableJob.class);
        jobTypes.put("TestCallableJob", TestCallableJob.class);
        final MapBasedJobFactory jobFactory = new MapBasedJobFactory(jobTypes);
        Assert.assertEquals(jobTypes, jobFactory.getJobTypes());
        jobFactory.removeJobType(TestCallableJob.class);
        jobTypes.values().remove(TestCallableJob.class);
        Assert.assertEquals(jobTypes, jobFactory.getJobTypes());
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testRemoveJobType_Null() {
        final Map<String, Class<?>> jobTypes = new HashMap<String, Class<?>>();
        jobTypes.put("TestRunnableJob", TestRunnableJob.class);
        jobTypes.put("TestCallableJob", TestCallableJob.class);
        final MapBasedJobFactory jobFactory = new MapBasedJobFactory(jobTypes);
        Assert.assertEquals(jobTypes, jobFactory.getJobTypes());
        jobFactory.removeJobType(null);
    }
    
    @Test
    public void testMaterializeJob_Types() throws Exception {
        final Map<String, Class<?>> jobTypes = new HashMap<String, Class<?>>();
        jobTypes.put("TestRunnableJob", TestRunnableJob.class);
        jobTypes.put("TestCallableJob", TestCallableJob.class);
        final MapBasedJobFactory jobFactory = new MapBasedJobFactory(jobTypes);
        final Object action = jobFactory.materializeJob(new Job("TestRunnableJob"));
        Assert.assertNotNull(action);
        Assert.assertEquals(TestRunnableJob.class, action.getClass());
        final Object action2 = jobFactory.materializeJob(new Job("TestCallableJob"));
        Assert.assertNotNull(action2);
        Assert.assertEquals(TestCallableJob.class, action2.getClass());
    }
    
    @Test(expected = UnpermittedJobException.class)
    public void testMaterializeJob_Types_NotPermitted() throws Exception {
        final Map<String, Class<?>> jobTypes = new HashMap<String, Class<?>>();
        final MapBasedJobFactory jobFactory = new MapBasedJobFactory(jobTypes);
        jobFactory.materializeJob(new Job("TestRunnableJob"));
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
