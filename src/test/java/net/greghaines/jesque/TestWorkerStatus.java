package net.greghaines.jesque;

import static com.google.common.truth.Truth.assertThat;

import java.util.Date;
import org.junit.Test;

public class TestWorkerStatus {

  @Test
  public void testConstructor_NoArg() {
    final WorkerStatus status = new WorkerStatus();
    assertThat(status.getRunAt()).isNull();
    assertThat(status.getQueue()).isNull();
    assertThat(status.getPayload()).isNull();
    assertThat(status.isPaused()).isFalse();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_Clone_Null() {
    new WorkerStatus(null);
  }

  @Test
  public void testConstructor_Clone() {
    final WorkerStatus protoStatus = new WorkerStatus();
    final Date runAt = new Date();
    protoStatus.setRunAt(runAt);
    assertThat(protoStatus.getRunAt()).isEqualTo(runAt);
    final String queue = "queue1";
    protoStatus.setQueue(queue);
    assertThat(protoStatus.getQueue()).isEqualTo(queue);
    final Job payload = new Job("clazz", 1, 2.0);
    protoStatus.setPayload(payload);
    assertThat(protoStatus.getPayload()).isEqualTo(payload);
    final boolean paused = true;
    protoStatus.setPaused(paused);
    assertThat(protoStatus.isPaused()).isEqualTo(paused);
    final WorkerStatus status = new WorkerStatus(protoStatus);
    TestUtils.assertFullyEquals(protoStatus, status);
    assertThat(status.getRunAt()).isEqualTo(runAt);
    assertThat(status.getPayload()).isEqualTo(payload);
    assertThat(status.getQueue()).isEqualTo(queue);
    assertThat(status.isPaused()).isEqualTo(paused);
  }

  @Test
  public void testEquals() {
    final WorkerStatus status1 = new WorkerStatus();
    TestUtils.assertFullyEquals(status1, status1);
    assertThat(status1.equals(null)).isFalse();
    assertThat(status1).isNotEqualTo(new Object());
    final WorkerStatus status2 = new WorkerStatus();
    TestUtils.assertFullyEquals(status1, status2);
    status2.setPaused(true);
    assertThat(status1).isNotEqualTo(status2);
    status1.setPaused(true);
    TestUtils.assertFullyEquals(status1, status2);
    status2.setQueue("queue1");
    assertThat(status1).isNotEqualTo(status2);
    status1.setQueue("queue2");
    assertThat(status1).isNotEqualTo(status2);
    status1.setQueue("queue1");
    TestUtils.assertFullyEquals(status1, status2);
    final Date runAt1 = new Date(2L);
    status2.setRunAt(runAt1);
    assertThat(status1).isNotEqualTo(status2);
    status1.setRunAt(new Date(3L));
    assertThat(status1).isNotEqualTo(status2);
    status1.setRunAt(runAt1);
    TestUtils.assertFullyEquals(status1, status2);
    final Job payload1 = new Job();
    status2.setPayload(payload1);
    assertThat(status1).isNotEqualTo(status2);
    status1.setPayload(new Job("foo"));
    assertThat(status1).isNotEqualTo(status2);
    status1.setPayload(payload1);
    TestUtils.assertFullyEquals(status1, status2);
  }
}
