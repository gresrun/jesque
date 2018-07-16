package net.greghaines.jesque.meta;

import java.util.Arrays;
import java.util.List;

import net.greghaines.jesque.Job;

import org.junit.Assert;
import org.junit.Test;

public class TestQueueInfo {

    @Test
    public void testProperties() {
        final QueueInfo qInfo = new QueueInfo();
        final String name = "foo";
        qInfo.setName(name);
        Assert.assertEquals(name, qInfo.getName());
        Assert.assertEquals(name, qInfo.toString());
        final Long size = 3l;
        qInfo.setSize(size);
        Assert.assertEquals(size, qInfo.getSize());
        final List<Job> jobs = Arrays.asList(new Job());
        qInfo.setJobs(jobs);
        Assert.assertEquals(jobs, qInfo.getJobs());
        final boolean delayed = true;
        qInfo.setDelayed(delayed);
        Assert.assertEquals(delayed, qInfo.isDelayed());
    }
    
    @Test
    public void testCompareToEqualsHashCode() {
        final QueueInfo qi1 = new QueueInfo();
        Assert.assertTrue(qi1.compareTo(null) > 0);
        Assert.assertFalse(qi1.equals(null));
        Assert.assertTrue(qi1.equals(qi1));
        final QueueInfo qi2 = new QueueInfo();
        Assert.assertEquals(0, qi1.compareTo(qi2));
        Assert.assertTrue(qi1.equals(qi2));
        Assert.assertEquals(qi1.hashCode(), qi2.hashCode());
        qi1.setName("foo");
        Assert.assertTrue(qi1.compareTo(qi2) > 0);
        Assert.assertFalse(qi1.equals(qi2));
        qi1.setName(null);
        qi2.setName("foo");
        Assert.assertTrue(qi1.compareTo(qi2) < 0);
        Assert.assertFalse(qi1.equals(qi2));
        qi1.setName("foo");
        Assert.assertEquals(0, qi1.compareTo(qi2));
        Assert.assertTrue(qi1.equals(qi2));
        Assert.assertEquals(qi1.hashCode(), qi2.hashCode());
        qi1.setName("bar");
        Assert.assertTrue(qi1.compareTo(qi2) < 0);
        Assert.assertFalse(qi1.equals(qi2));
        qi1.setName("qux");
        Assert.assertTrue(qi1.compareTo(qi2) > 0);
        Assert.assertFalse(qi1.equals(qi2));
    }
}
