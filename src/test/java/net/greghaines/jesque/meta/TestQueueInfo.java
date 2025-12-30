package net.greghaines.jesque.meta;

import static com.google.common.truth.Truth.assertThat;

import java.util.Arrays;
import java.util.List;
import net.greghaines.jesque.Job;
import org.junit.Test;

public class TestQueueInfo {

  @Test
  public void testProperties() {
    final QueueInfo qInfo = new QueueInfo();
    final String name = "foo";
    qInfo.setName(name);
    assertThat(qInfo.getName()).isEqualTo(name);
    assertThat(qInfo.toString()).isEqualTo(name);
    final Long size = 3l;
    qInfo.setSize(size);
    assertThat(qInfo.getSize()).isEqualTo(size);
    final List<Job> jobs = Arrays.asList(new Job());
    qInfo.setJobs(jobs);
    assertThat(qInfo.getJobs()).isEqualTo(jobs);
    final boolean delayed = true;
    qInfo.setDelayed(delayed);
    assertThat(qInfo.isDelayed()).isEqualTo(delayed);
  }

  @Test
  public void testCompareToEqualsHashCode() {
    final QueueInfo qi1 = new QueueInfo();
    assertThat(qi1.compareTo(null)).isGreaterThan(0);
    assertThat(qi1.equals(null)).isFalse();
    assertThat(qi1).isEqualTo(qi1);
    final QueueInfo qi2 = new QueueInfo();
    assertThat(qi1).isEquivalentAccordingToCompareTo(qi2);
    assertThat(qi1).isEqualTo(qi2);
    assertThat(qi1.hashCode()).isEqualTo(qi2.hashCode());
    qi1.setName("foo");
    assertThat(qi1).isGreaterThan(qi2);
    assertThat(qi1).isNotEqualTo(qi2);
    qi1.setName(null);
    qi2.setName("foo");
    assertThat(qi1).isLessThan(qi2);
    assertThat(qi1).isNotEqualTo(qi2);
    qi1.setName("foo");
    assertThat(qi1).isEquivalentAccordingToCompareTo(qi2);
    assertThat(qi1).isEqualTo(qi2);
    assertThat(qi1.hashCode()).isEqualTo(qi2.hashCode());
    qi1.setName("bar");
    assertThat(qi1).isLessThan(qi2);
    assertThat(qi1).isNotEqualTo(qi2);
    qi1.setName("qux");
    assertThat(qi1).isGreaterThan(qi2);
    assertThat(qi1).isNotEqualTo(qi2);
  }
}
