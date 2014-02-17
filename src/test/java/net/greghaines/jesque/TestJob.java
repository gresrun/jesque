package net.greghaines.jesque;

import static net.greghaines.jesque.utils.JesqueUtils.entry;
import static net.greghaines.jesque.utils.JesqueUtils.map;

import java.util.Arrays;
import java.util.Map;

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

    @Test(expected=IllegalArgumentException.class)
    public void testConstructor_NamedArgs_NullName() {
        new Job(null, map(entry("foo", "bar")));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testConstructor_NamedArgs_EmptyName() {
        new Job("", map(entry("foo", "bar")));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testConstructor_AllArgs_NullName() {
        new Job(null, new Object[]{true, 1}, map(entry("foo", "bar")));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testConstructor_AllArgs_EmptyName() {
        new Job("", new Object[]{true, 1}, map(entry("foo", "bar")));
    }

    @Test
    public void testConstructor_NoArg() {
        final Job job = new Job();
        Assert.assertNull(job.getClassName());
        Assert.assertNull(job.getArgs());
        Assert.assertNull(job.getVars());
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
        Assert.assertNull(job.getVars());
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
        Assert.assertNull(job.getVars());
        final Object[] args = job.getArgs();
        Assert.assertNotNull(args);
        Assert.assertArrayEquals(new Object[]{arg1, arg2}, args);
        Assert.assertTrue(job.isValid());
        Assert.assertNotNull(job.toString());
    }

    @Test
    public void testConstructor_NamedArgs() {
        final String className = "foo";
        final Map<String,? extends Object> vars = map(entry("foo", "bar"), entry("baz", 123));
        final Job job = new Job(className, vars);
        Assert.assertEquals(className, job.getClassName());
        Assert.assertNull(job.getArgs());
        Assert.assertEquals(vars, job.getVars());
        Assert.assertTrue(job.isValid());
        Assert.assertNotNull(job.toString());
    }

    @Test
    public void testConstructor_AllArgs() {
        final String className = "foo";
        final Object[] args = {1, 2.0};
        final Map<String,? extends Object> vars = map(entry("foo", "bar"), entry("baz", 123));
        final Job job = new Job(className, args, vars);
        Assert.assertEquals(className, job.getClassName());
        Assert.assertNotNull(job.getArgs());
        Assert.assertArrayEquals(args, job.getArgs());
        Assert.assertEquals(vars, job.getVars());
        Assert.assertTrue(job.isValid());
        Assert.assertNotNull(job.toString());
    }

    @Test
    public void testConstructor_Clone() {
        final String className = "foo";
        final Object[] args = {1, 2.0};
        final Map<String,? extends Object> vars = map(entry("foo", "bar"), entry("baz", 123));
        final Job protoJob = new Job(className, args, vars);
        final Job job = new Job(protoJob);
        Assert.assertEquals(className, job.getClassName());
        final Object[] newArgs = job.getArgs();
        Assert.assertNotNull(newArgs);
        Assert.assertArrayEquals(args, newArgs);
        Assert.assertEquals(vars, job.getVars());
        Assert.assertEquals(protoJob.isValid(), job.isValid());
        TestUtils.assertFullyEquals(protoJob, job);
        final Job protoJob2 = new Job(className, null, null);
        final Job job2 = new Job(protoJob2);
        Assert.assertEquals(className, job2.getClassName());
        Assert.assertNull(job2.getArgs());
        Assert.assertNull(job2.getVars());
        Assert.assertEquals(protoJob2.isValid(), job2.isValid());
        TestUtils.assertFullyEquals(protoJob2, job2);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testConstructor_Clone_Null() {
        new Job((Job)null);
    }

    @Test
    public void testSetters() {
        final String className1 = "foo";
        final String className2 = "";
        final Object arg1 = 1;
        final Object arg2 = 2.0;
        final Map<String,? extends Object> vars = map(entry("foo", "bar"), entry("baz", 123));
        final Job job = new Job();
        Assert.assertNull(job.getClassName());
        Assert.assertNull(job.getArgs());
        Assert.assertNull(job.getVars());
        Assert.assertFalse(job.isValid());
        job.setArgs(arg1, arg2);
        Assert.assertArrayEquals(new Object[]{arg1, arg2}, job.getArgs());
        Assert.assertFalse(job.isValid());
        job.setVars(vars);
        Assert.assertEquals(vars, job.getVars());
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
        final Map<String,? extends Object> vars = map(entry("foo", "bar"), entry("baz", 123));
        job2.setVars(vars);
        Assert.assertFalse(job1.equals(job2));
        job1.setVars(vars);
        TestUtils.assertFullyEquals(job1, job2);
    }
}
