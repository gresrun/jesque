package net.greghaines.jesque;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

public class TestJob {

    @Test(expected=IllegalArgumentException.class)
    public void testConstructor_VarArgs_NullName() {
        new Job(null, 1, 2.0);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testConstructor_VarArgs_EmptyName() {
        new Job("", 1, 2.0);
    }

    @Test
    public void testConstructor_NoArg() {
        final Job job = new Job();
        Assert.assertNull(job.getClassName());
        Assert.assertNull(job.getArgs());
        Assert.assertFalse(job.isValid());
        Assert.assertNotNull(job.toString());
    }

    @Test
    public void testConstructor_VarArgs() {
        final String className = "foo";
        final Object arg1 = 1;
        final Object arg2 = 2.0;
        final Job job = new Job(className, arg1, arg2);
        Assert.assertEquals(className, job.getClassName());
        final Object[] args = job.getArgs();
        Assert.assertNotNull(args);
        Assert.assertArrayEquals(new Object[]{arg1, arg2}, args);
        Assert.assertTrue(job.isValid());
        Assert.assertNotNull(job.toString());
    }

    @Test
    public void testConstructor_ListArgs() {
        final String className = "foo";
        final Object arg1 = 1;
        final Object arg2 = 2.0;
        final Job job = new Job(className, Arrays.asList(arg1, arg2));
        Assert.assertEquals(className, job.getClassName());
        final Object[] args = job.getArgs();
        Assert.assertNotNull(args);
        Assert.assertArrayEquals(new Object[]{arg1, arg2}, args);
        Assert.assertTrue(job.isValid());
        Assert.assertNotNull(job.toString());
    }

    @Test
    public void testConstructor_Clone() {
        final String className = "foo";
        final Object arg1 = 1;
        final Object arg2 = 2.0;
        final Job protoJob = new Job(className, arg1, arg2);
        final Job job = new Job(protoJob);
        Assert.assertEquals(className, job.getClassName());
        final Object[] args = job.getArgs();
        Assert.assertNotNull(args);
        Assert.assertArrayEquals(new Object[]{arg1, arg2}, args);
        Assert.assertTrue(job.isValid());
        TestUtils.assertFullyEquals(protoJob, job);
    }

    @Test
    public void testSetters() {
        final String className1 = "foo";
        final String className2 = "";
        final Object arg1 = 1;
        final Object arg2 = 2.0;
        final Job job = new Job();
        Assert.assertNull(job.getClassName());
        Assert.assertNull(job.getArgs());
        Assert.assertFalse(job.isValid());
        job.setArgs(arg1, arg2);
        Assert.assertArrayEquals(new Object[]{arg1, arg2}, job.getArgs());
        Assert.assertFalse(job.isValid());
        job.setClassName(className1);
        Assert.assertEquals(className1, job.getClassName());
        Assert.assertTrue(job.isValid());
        job.setClassName(className2);
        Assert.assertEquals(className2, job.getClassName());
        Assert.assertFalse(job.isValid());
    }
    
    @Test
    public void testEquals() {
        final Job job1 = new Job();
        TestUtils.assertFullyEquals(job1, job1);
        Assert.assertFalse(job1.equals(null));
        Assert.assertFalse(job1.equals(new Object()));
        final Job job2 = new Job();
        TestUtils.assertFullyEquals(job1, job2);
        job2.setClassName("foo");
        Assert.assertFalse(job1.equals(job2));
        job1.setClassName("bar");
        Assert.assertFalse(job1.equals(job2));
        job1.setClassName("foo");
        TestUtils.assertFullyEquals(job1, job2);
        final Object arg1 = 1;
        final Object arg2 = 2.0;
        job2.setArgs(arg1, arg2);
        Assert.assertFalse(job1.equals(job2));
        job1.setArgs(arg1, arg2);
        TestUtils.assertFullyEquals(job1, job2);
    }
}
