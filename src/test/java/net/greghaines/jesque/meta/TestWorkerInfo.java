package net.greghaines.jesque.meta;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import net.greghaines.jesque.WorkerStatus;
import net.greghaines.jesque.meta.WorkerInfo.State;

import org.junit.Assert;
import org.junit.Test;

public class TestWorkerInfo {

    @Test
    public void testProperties() {
        final WorkerInfo wInfo = new WorkerInfo();
        final String name = "foo";
        wInfo.setName(name);
        Assert.assertEquals(name, wInfo.getName());
        Assert.assertEquals(name, wInfo.toString());
        final State state = State.IDLE;
        wInfo.setState(state);
        Assert.assertEquals(state, wInfo.getState());
        final Date started = new Date();
        wInfo.setStarted(started);
        Assert.assertEquals(started, wInfo.getStarted());
        final Long processed = 3l;
        wInfo.setProcessed(processed);
        Assert.assertEquals(processed, wInfo.getProcessed());
        final Long failed = 4l;
        wInfo.setFailed(failed);
        Assert.assertEquals(failed, wInfo.getFailed());
        final String host = "bar";
        wInfo.setHost(host);
        Assert.assertEquals(host, wInfo.getHost());
        final String pid = "123";
        wInfo.setPid(pid);
        Assert.assertEquals(pid, wInfo.getPid());
        final List<String> queues = Arrays.asList("queue1", "queue2");
        wInfo.setQueues(queues);
        Assert.assertEquals(queues, wInfo.getQueues());
        final WorkerStatus status = new WorkerStatus();
        wInfo.setStatus(status);
        Assert.assertEquals(status, wInfo.getStatus());
    }
    
    @Test
    public void testCompareToEqualsHashCode() {
        final WorkerInfo wi1 = new WorkerInfo();
        Assert.assertTrue(wi1.compareTo(null) > 0);
        Assert.assertFalse(wi1.equals(null));
        Assert.assertTrue(wi1.equals(wi1));
        final WorkerInfo wi2 = new WorkerInfo();
        Assert.assertEquals(0, wi1.compareTo(wi2));
        Assert.assertTrue(wi1.equals(wi2));
        Assert.assertEquals(wi1.hashCode(), wi2.hashCode());
        final WorkerStatus status1 = new WorkerStatus();
        wi1.setStatus(status1);
        Assert.assertTrue(wi1.compareTo(wi2) > 0);
        Assert.assertFalse(wi1.equals(wi2));
        wi2.setStatus(status1);
        Assert.assertEquals(0, wi1.compareTo(wi2));
        Assert.assertTrue(wi1.equals(wi2));
        Assert.assertEquals(wi1.hashCode(), wi2.hashCode());
        wi1.setStatus(null);
        Assert.assertTrue(wi1.compareTo(wi2) < 0);
        Assert.assertFalse(wi1.equals(wi2));
        wi1.setStatus(status1);
        final Date runAt1 = new Date();
        status1.setRunAt(runAt1);
        Assert.assertEquals(0, wi1.compareTo(wi2));
        Assert.assertTrue(wi1.equals(wi2));
        Assert.assertEquals(wi1.hashCode(), wi2.hashCode());
        final WorkerStatus status2 = new WorkerStatus();
        wi2.setStatus(status2);
        Assert.assertTrue(wi1.compareTo(wi2) > 0);
        Assert.assertFalse(wi1.equals(wi2));
        final Date runAt2 = new Date(runAt1.getTime() + 1000);
        status2.setRunAt(runAt2);
        Assert.assertTrue(wi1.compareTo(wi2) < 0);
        Assert.assertFalse(wi1.equals(wi2));
        status1.setRunAt(null);
        Assert.assertTrue(wi1.compareTo(wi2) < 0);
        Assert.assertFalse(wi1.equals(wi2));
    }
}
