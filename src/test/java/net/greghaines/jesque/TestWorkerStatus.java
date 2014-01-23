package net.greghaines.jesque;

import java.util.Date;

import org.junit.Assert;
import org.junit.Test;

public class TestWorkerStatus {

    @Test
    public void testConstructor_NoArg() {
        final WorkerStatus status = new WorkerStatus();
        Assert.assertNull(status.getRunAt());
        Assert.assertNull(status.getQueue());
        Assert.assertNull(status.getPayload());
        Assert.assertFalse(status.isPaused());
    }

    @Test(expected=IllegalArgumentException.class)
    public void testConstructor_Clone_Null() {
        new WorkerStatus(null);
    }

    @Test
    public void testConstructor_Clone() {
        final WorkerStatus protoStatus = new WorkerStatus();
        final Date runAt = new Date();
        protoStatus.setRunAt(runAt);
        Assert.assertEquals(runAt, protoStatus.getRunAt());
        final String queue = "queue1";
        protoStatus.setQueue(queue);
        Assert.assertEquals(queue, protoStatus.getQueue());
        final Job payload = new Job("clazz", 1, 2.0);
        protoStatus.setPayload(payload);
        Assert.assertEquals(payload, protoStatus.getPayload());
        final boolean paused = true;
        protoStatus.setPaused(paused);
        Assert.assertEquals(paused, protoStatus.isPaused());
        final WorkerStatus status = new WorkerStatus(protoStatus);
        TestUtils.assertFullyEquals(protoStatus, status);
        Assert.assertEquals(runAt, status.getRunAt());
        Assert.assertEquals(payload, status.getPayload());
        Assert.assertEquals(queue, status.getQueue());
        Assert.assertEquals(paused, status.isPaused());
    }
    
    @Test
    public void testEquals() {
        final WorkerStatus status1 = new WorkerStatus();
        TestUtils.assertFullyEquals(status1, status1);
        Assert.assertFalse(status1.equals(null));
        Assert.assertFalse(status1.equals(new Object()));
        final WorkerStatus status2 = new WorkerStatus();
        TestUtils.assertFullyEquals(status1, status2);
        status2.setPaused(true);
        Assert.assertFalse(status1.equals(status2));
        status1.setPaused(true);
        TestUtils.assertFullyEquals(status1, status2);
        status2.setQueue("queue1");
        Assert.assertFalse(status1.equals(status2));
        status1.setQueue("queue2");
        Assert.assertFalse(status1.equals(status2));
        status1.setQueue("queue1");
        TestUtils.assertFullyEquals(status1, status2);
        final Date runAt1 = new Date(2L);
        status2.setRunAt(runAt1);
        Assert.assertFalse(status1.equals(status2));
        status1.setRunAt(new Date(3L));
        Assert.assertFalse(status1.equals(status2));
        status1.setRunAt(runAt1);
        TestUtils.assertFullyEquals(status1, status2);
        final Job payload1 = new Job();
        status2.setPayload(payload1);
        Assert.assertFalse(status1.equals(status2));
        status1.setPayload(new Job("foo"));
        Assert.assertFalse(status1.equals(status2));
        status1.setPayload(payload1);
        TestUtils.assertFullyEquals(status1, status2);
    }
}
