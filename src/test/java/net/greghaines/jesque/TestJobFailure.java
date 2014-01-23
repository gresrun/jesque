package net.greghaines.jesque;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class TestJobFailure {

    @Test
    public void testConstructor_NoArg() {
        final JobFailure job = new JobFailure();
        Assert.assertNull(job.getWorker());
        Assert.assertNull(job.getQueue());
        Assert.assertNull(job.getPayload());
        Assert.assertNull(job.getException());
        Assert.assertNull(job.getExceptionString());
        Assert.assertNull(job.getError());
        Assert.assertNull(job.getBacktrace());
        Assert.assertNull(job.getFailedAt());
        Assert.assertNull(job.getRetriedAt());
    }

    @Test(expected=IllegalArgumentException.class)
    public void testConstructor_Clone_Null() {
        new JobFailure(null);
    }

    @Test
    public void testConstructor_Clone() {
        final JobFailure protoFail = new JobFailure();
        final String error = "error1";
        protoFail.setError(error);
        Assert.assertEquals(error, protoFail.getError());
        final List<String> bTrace = Arrays.asList("btrace1", "btrace2");
        protoFail.setBacktrace(bTrace);
        Assert.assertEquals(bTrace, protoFail.getBacktrace());
        final String exStr = "BOOM";
        protoFail.setExceptionString(exStr);
        Assert.assertEquals(exStr, protoFail.getExceptionString());
        final Throwable ex = new RuntimeException(exStr);
        protoFail.setException(ex);
        Assert.assertEquals(ex, protoFail.getException());
        final Date failedAt = new Date();
        protoFail.setFailedAt(failedAt);
        Assert.assertEquals(failedAt, protoFail.getFailedAt());
        final Job payload = new Job("clazz", 1, 2.0);
        protoFail.setPayload(payload);
        Assert.assertEquals(payload, protoFail.getPayload());
        final String queue = "queue1";
        protoFail.setQueue(queue);
        Assert.assertEquals(queue, protoFail.getQueue());
        final Date retriedAt = new Date();
        protoFail.setRetriedAt(retriedAt);
        Assert.assertEquals(retriedAt, protoFail.getRetriedAt());
        final String worker = "worker1";
        protoFail.setWorker(worker);
        Assert.assertEquals(worker, protoFail.getWorker());
        final JobFailure fail = new JobFailure(protoFail);
        TestUtils.assertFullyEquals(protoFail, fail);
        Assert.assertEquals(error, fail.getError());
        Assert.assertEquals(bTrace, fail.getBacktrace());
        Assert.assertEquals(exStr, fail.getExceptionString());
        Assert.assertEquals(ex, fail.getException());
        Assert.assertEquals(failedAt, fail.getFailedAt());
        Assert.assertEquals(payload, fail.getPayload());
        Assert.assertEquals(queue, fail.getQueue());
        Assert.assertEquals(retriedAt, fail.getRetriedAt());
        Assert.assertEquals(worker, fail.getWorker());
    }
    
    @Test
    public void testEquals() {
        final JobFailure fail1 = new JobFailure();
        TestUtils.assertFullyEquals(fail1, fail1);
        Assert.assertFalse(fail1.equals(null));
        Assert.assertFalse(fail1.equals(new Object()));
        final JobFailure fail2 = new JobFailure();
        TestUtils.assertFullyEquals(fail1, fail2);
        fail2.setQueue("queue1");
        Assert.assertFalse(fail1.equals(fail2));
        fail1.setQueue("queue2");
        Assert.assertFalse(fail1.equals(fail2));
        fail1.setQueue("queue1");
        TestUtils.assertFullyEquals(fail1, fail2);
        fail2.setWorker("worker1");
        Assert.assertFalse(fail1.equals(fail2));
        fail1.setWorker("worker2");
        Assert.assertFalse(fail1.equals(fail2));
        fail1.setWorker("worker1");
        TestUtils.assertFullyEquals(fail1, fail2);
        fail2.setExceptionString("ex1");
        Assert.assertFalse(fail1.equals(fail2));
        fail1.setExceptionString("ex2");
        Assert.assertFalse(fail1.equals(fail2));
        fail1.setExceptionString("ex1");
        TestUtils.assertFullyEquals(fail1, fail2);
        fail2.setError("error1");
        Assert.assertFalse(fail1.equals(fail2));
        fail1.setError("error2");
        Assert.assertFalse(fail1.equals(fail2));
        fail1.setError("error1");
        TestUtils.assertFullyEquals(fail1, fail2);
        final Date failedAt1 = new Date(0L);
        fail2.setFailedAt(failedAt1);
        Assert.assertFalse(fail1.equals(fail2));
        fail1.setFailedAt(new Date(1L));
        Assert.assertFalse(fail1.equals(fail2));
        fail1.setFailedAt(failedAt1);
        TestUtils.assertFullyEquals(fail1, fail2);
        final Date retriedAt1 = new Date(2L);
        fail2.setRetriedAt(retriedAt1);
        Assert.assertFalse(fail1.equals(fail2));
        fail1.setRetriedAt(new Date(3L));
        Assert.assertFalse(fail1.equals(fail2));
        fail1.setRetriedAt(retriedAt1);
        TestUtils.assertFullyEquals(fail1, fail2);
        final Exception ex1 = new Exception("BOOM");
        fail2.setException(ex1);
        Assert.assertFalse(fail1.equals(fail2));
        fail1.setException(ex1);
        TestUtils.assertFullyEquals(fail1, fail2);
        final Job payload1 = new Job();
        fail2.setPayload(payload1);
        Assert.assertFalse(fail1.equals(fail2));
        fail1.setPayload(new Job("foo"));
        Assert.assertFalse(fail1.equals(fail2));
        fail1.setPayload(payload1);
        TestUtils.assertFullyEquals(fail1, fail2);
        final List<String> bTrace = Arrays.asList("bTrace1", "bTrace2");
        fail2.setBacktrace(bTrace);
        Assert.assertFalse(fail1.equals(fail2));
        fail1.setBacktrace(Arrays.asList("bTrace3"));
        Assert.assertFalse(fail1.equals(fail2));
        fail1.setBacktrace(bTrace);
        TestUtils.assertFullyEquals(fail1, fail2);
    }
}
