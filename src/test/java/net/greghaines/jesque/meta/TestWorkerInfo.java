package net.greghaines.jesque.meta;

import static com.google.common.truth.Truth.assertThat;

import java.util.Date;
import java.util.List;
import net.greghaines.jesque.WorkerStatus;
import net.greghaines.jesque.meta.WorkerInfo.State;
import org.junit.Test;

public class TestWorkerInfo {

  @Test
  public void testProperties() {
    final WorkerInfo wInfo = new WorkerInfo();
    final String name = "foo";
    wInfo.setName(name);
    assertThat(wInfo.getName()).isEqualTo(name);
    assertThat(wInfo.toString()).isEqualTo(name);
    final State state = State.IDLE;
    wInfo.setState(state);
    assertThat(wInfo.getState()).isEqualTo(state);
    final Date started = new Date();
    wInfo.setStarted(started);
    assertThat(wInfo.getStarted()).isEqualTo(started);
    final Long processed = 3l;
    wInfo.setProcessed(processed);
    assertThat(wInfo.getProcessed()).isEqualTo(processed);
    final Long failed = 4l;
    wInfo.setFailed(failed);
    assertThat(wInfo.getFailed()).isEqualTo(failed);
    final String host = "bar";
    wInfo.setHost(host);
    assertThat(wInfo.getHost()).isEqualTo(host);
    final String pid = "123";
    wInfo.setPid(pid);
    assertThat(wInfo.getPid()).isEqualTo(pid);
    final List<String> queues = List.of("queue1", "queue2");
    wInfo.setQueues(queues);
    assertThat(wInfo.getQueues()).isEqualTo(queues);
    final WorkerStatus status = new WorkerStatus();
    wInfo.setStatus(status);
    assertThat(wInfo.getStatus()).isEqualTo(status);
  }

  @Test
  public void testCompareToEqualsHashCode() {
    final WorkerInfo wi1 = new WorkerInfo();
    assertThat(wi1.compareTo(null)).isGreaterThan(0);
    assertThat(wi1.equals(null)).isFalse();
    assertThat(wi1).isEqualTo(wi1);
    final WorkerInfo wi2 = new WorkerInfo();
    assertThat(wi1).isEquivalentAccordingToCompareTo(wi2);
    assertThat(wi1).isEqualTo(wi2);
    assertThat(wi1.hashCode()).isEqualTo(wi2.hashCode());
    final WorkerStatus status1 = new WorkerStatus();
    wi1.setStatus(status1);
    assertThat(wi1).isGreaterThan(wi2);
    assertThat(wi1.equals(wi2)).isFalse();
    wi2.setStatus(status1);
    assertThat(wi1).isEquivalentAccordingToCompareTo(wi2);
    assertThat(wi1).isEqualTo(wi2);
    assertThat(wi1.hashCode()).isEqualTo(wi2.hashCode());
    wi1.setStatus(null);
    assertThat(wi1).isLessThan(wi2);
    assertThat(wi1).isNotEqualTo(wi2);
    wi1.setStatus(status1);
    final Date runAt1 = new Date();
    status1.setRunAt(runAt1);
    assertThat(wi1).isEquivalentAccordingToCompareTo(wi2);
    assertThat(wi1).isEqualTo(wi2);
    assertThat(wi1.hashCode()).isEqualTo(wi2.hashCode());
    final WorkerStatus status2 = new WorkerStatus();
    wi2.setStatus(status2);
    assertThat(wi1).isGreaterThan(wi2);
    assertThat(wi1).isNotEqualTo(wi2);
    final Date runAt2 = new Date(runAt1.getTime() + 1000);
    status2.setRunAt(runAt2);
    assertThat(wi1).isLessThan(wi2);
    assertThat(wi1).isNotEqualTo(wi2);
    status1.setRunAt(null);
    assertThat(wi1).isLessThan(wi2);
    assertThat(wi1).isNotEqualTo(wi2);
  }
}
