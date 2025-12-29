package net.greghaines.jesque.utils;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import net.greghaines.jesque.Job;
import net.greghaines.jesque.worker.UnpermittedJobException;

import org.junit.Assert;
import org.junit.Test;

public class TestJesqueUtils {

    @Test
    public void testJoin_VarArgs() {
        Assert.assertEquals("foo,bar,baz", JesqueUtils.join(",", "foo", "bar", "baz"));
    }

    @Test
    public void testJoin_Iterable() {
        Assert.assertEquals("foo,bar,baz",
                JesqueUtils.join(",", Arrays.asList("foo", "bar", "baz")));
    }

    @Test
    public void testCreateKey_VarArgs() {
        Assert.assertEquals("bar:baz", JesqueUtils.createKey("", "bar", "baz"));
    }

    @Test
    public void testCreateKey_Iterable() {
        Assert.assertEquals("foo:bar:baz",
                JesqueUtils.createKey("foo", Arrays.asList("bar", "baz")));
    }

    @Test
    public void testMaterializeJob() throws Exception {
        final Object action = JesqueUtils.materializeJob(new Job(TestRunnableJob.class.getName()));
        Assert.assertNotNull(action);
        Assert.assertEquals(TestRunnableJob.class, action.getClass());
        final Object action2 = JesqueUtils.materializeJob(new Job(TestCallableJob.class.getName()));
        Assert.assertNotNull(action2);
        Assert.assertEquals(TestCallableJob.class, action2.getClass());
    }

    @Test(expected = ClassCastException.class)
    public void testMaterializeJob_NotRunnable() throws Exception {
        JesqueUtils.materializeJob(new Job(TestBadJob.class.getName()));
    }

    @Test
    public void testMaterializeJob_Types() throws Exception {
        final Map<String, Class<?>> jobTypes = new HashMap<String, Class<?>>();
        jobTypes.put("TestRunnableJob", TestRunnableJob.class);
        jobTypes.put("TestCallableJob", TestCallableJob.class);
        final Object action = JesqueUtils.materializeJob(new Job("TestRunnableJob"), jobTypes);
        Assert.assertNotNull(action);
        Assert.assertEquals(TestRunnableJob.class, action.getClass());
        final Object action2 = JesqueUtils.materializeJob(new Job("TestCallableJob"), jobTypes);
        Assert.assertNotNull(action2);
        Assert.assertEquals(TestCallableJob.class, action2.getClass());
    }

    @Test(expected = UnpermittedJobException.class)
    public void testMaterializeJob_Types_NotPermitted() throws Exception {
        final Map<String, Class<?>> jobTypes = new HashMap<String, Class<?>>();
        JesqueUtils.materializeJob(new Job("TestRunnableJob"), jobTypes);
    }

    @Test(expected = ClassCastException.class)
    public void testMaterializeJob_Types_NotRunnable() throws Exception {
        final Map<String, Class<?>> jobTypes = new HashMap<String, Class<?>>();
        jobTypes.put("TestBadJob", TestBadJob.class);
        JesqueUtils.materializeJob(new Job("TestBadJob"), jobTypes);
    }

    @Test
    public void testRecreateStackTrace() throws ParseException {
        final List<String> bTrace = new ArrayList<String>(
                Arrays.asList("foo", "\tat net.greghaines.jesque.Job.<init>(Job.java:30)",
                        "\tat net.greghaines.jesque.Job.<init>(Job.java)",
                        "\tat net.greghaines.jesque.Job.<init>(Unknown Source)",
                        "\tat net.greghaines.jesque.Job.<init>(Native Method)"));
        final StackTraceElement[] stes = JesqueUtils.recreateStackTrace(bTrace);
        Assert.assertEquals(4, stes.length);
    }

    @Test(expected = ParseException.class)
    public void testRecreateStackTrace_BadFormat() throws ParseException {
        final List<String> bTrace = new ArrayList<String>(Arrays.asList("\tat net.greghaines"));
        final StackTraceElement[] stes = JesqueUtils.recreateStackTrace(bTrace);
        Assert.assertEquals(0, stes.length);
    }

    @Test
    public void testRecreateStackTrace_NotFormat() throws ParseException {
        final List<String> bTrace = Arrays.asList("foo", "bar");
        final StackTraceElement[] stes = JesqueUtils.recreateStackTrace(bTrace);
        Assert.assertEquals(0, stes.length);
    }

    @Test
    public void testRecreateStackTrace_Empty() throws ParseException {
        final StackTraceElement[] stes = JesqueUtils.recreateStackTrace(new ArrayList<String>());
        Assert.assertEquals(0, stes.length);
    }

    @Test
    public void testRecreateStackTrace_Null() throws ParseException {
        final StackTraceElement[] stes = JesqueUtils.recreateStackTrace(null);
        Assert.assertEquals(0, stes.length);
    }

    @Test
    public void testEqual() {
        Throwable ex1 = new RuntimeException();
        Assert.assertTrue(JesqueUtils.equal(ex1, ex1));
        Assert.assertFalse(JesqueUtils.equal(ex1, null));
        Assert.assertFalse(JesqueUtils.equal(null, ex1));
        Throwable ex2 = new Exception();
        Assert.assertFalse(JesqueUtils.equal(ex1, ex2));
        ex2 = new RuntimeException();
        Assert.assertTrue(JesqueUtils.equal(ex1, ex2));
        ex2 = new RuntimeException("foo");
        Assert.assertFalse(JesqueUtils.equal(ex1, ex2));
        ex1 = new RuntimeException("bar");
        Assert.assertFalse(JesqueUtils.equal(ex1, ex2));
        ex1 = new RuntimeException("foo");
        Assert.assertTrue(JesqueUtils.equal(ex1, ex2));
        ex1 = new RuntimeException("foo", new Exception());
        Assert.assertFalse(JesqueUtils.equal(ex1, ex2));
        ex2 = new RuntimeException("foo", new Exception());
        Assert.assertTrue(JesqueUtils.equal(ex1, ex2));
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
