package net.greghaines.jesque;

import static com.google.common.truth.Truth.assertThat;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import org.junit.Test;

public class TestJobFailure {

  @Test
  public void testConstructor_NoArg() {
    final JobFailure job = new JobFailure();
    assertThat(job.getWorker()).isNull();
    assertThat(job.getQueue()).isNull();
    assertThat(job.getPayload()).isNull();
    assertThat(job.getThrowable()).isNull();
    assertThat(job.getThrowableString()).isNull();
    assertThat(job.getError()).isNull();
    assertThat(job.getBacktrace()).isNull();
    assertThat(job.getFailedAt()).isNull();
    assertThat(job.getRetriedAt()).isNull();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_Clone_Null() {
    new JobFailure(null);
  }

  @Test
  public void testConstructor_Clone() {
    final JobFailure protoFail = new JobFailure();
    final String error = "error1";
    protoFail.setError(error);
    assertThat(protoFail.getError()).isEqualTo(error);
    final List<String> bTrace = Arrays.asList("btrace1", "btrace2");
    protoFail.setBacktrace(bTrace);
    assertThat(protoFail.getBacktrace()).isEqualTo(bTrace);
    final String tStr = "BOOM";
    protoFail.setThrowableString(tStr);
    assertThat(protoFail.getThrowableString()).isEqualTo(tStr);
    final Throwable t = new Exception(tStr);
    protoFail.setThrowable(t);
    assertThat(protoFail.getThrowable()).isEqualTo(t);
    final Throwable t1 = null; // new Error(tStr);
    protoFail.setThrowable(t1);
    assertThat(protoFail.getThrowable()).isEqualTo(t1);
    final Date failedAt = new Date();
    protoFail.setFailedAt(failedAt);
    assertThat(protoFail.getFailedAt()).isEqualTo(failedAt);
    final Job payload = new Job("clazz", 1, 2.0);
    protoFail.setPayload(payload);
    assertThat(protoFail.getPayload()).isEqualTo(payload);
    final String queue = "queue1";
    protoFail.setQueue(queue);
    assertThat(protoFail.getQueue()).isEqualTo(queue);
    final Date retriedAt = new Date();
    protoFail.setRetriedAt(retriedAt);
    assertThat(protoFail.getRetriedAt()).isEqualTo(retriedAt);
    final String worker = "worker1";
    protoFail.setWorker(worker);
    assertThat(protoFail.getWorker()).isEqualTo(worker);
    final JobFailure fail = new JobFailure(protoFail);
    TestUtils.assertFullyEquals(protoFail, fail);
    assertThat(fail.getError()).isEqualTo(error);
    assertThat(fail.getBacktrace()).isEqualTo(bTrace);
    assertThat(fail.getThrowableString()).isEqualTo(tStr);
    assertThat(fail.getThrowable()).isEqualTo(t1);
    assertThat(fail.getFailedAt()).isEqualTo(failedAt);
    assertThat(fail.getPayload()).isEqualTo(payload);
    assertThat(fail.getQueue()).isEqualTo(queue);
    assertThat(fail.getRetriedAt()).isEqualTo(retriedAt);
    assertThat(fail.getWorker()).isEqualTo(worker);
  }

  @Test
  public void testEquals() {
    final JobFailure fail1 = new JobFailure();
    TestUtils.assertFullyEquals(fail1, fail1);
    assertThat(fail1.equals(null)).isFalse();
    assertThat(fail1).isNotEqualTo(new Object());
    final JobFailure fail2 = new JobFailure();
    TestUtils.assertFullyEquals(fail1, fail2);
    fail2.setQueue("queue1");
    assertThat(fail1).isNotEqualTo(fail2);
    fail1.setQueue("queue2");
    assertThat(fail1).isNotEqualTo(fail2);
    fail1.setQueue("queue1");
    TestUtils.assertFullyEquals(fail1, fail2);
    fail2.setWorker("worker1");
    assertThat(fail1).isNotEqualTo(fail2);
    fail1.setWorker("worker2");
    assertThat(fail1).isNotEqualTo(fail2);
    fail1.setWorker("worker1");
    TestUtils.assertFullyEquals(fail1, fail2);
    fail2.setThrowableString("t1");
    assertThat(fail1).isNotEqualTo(fail2);
    fail1.setThrowableString("t2");
    assertThat(fail1).isNotEqualTo(fail2);
    fail1.setThrowableString("t1");
    TestUtils.assertFullyEquals(fail1, fail2);
    fail2.setError("error1");
    assertThat(fail1).isNotEqualTo(fail2);
    fail1.setError("error2");
    assertThat(fail1).isNotEqualTo(fail2);
    fail1.setError("error1");
    TestUtils.assertFullyEquals(fail1, fail2);
    final Date failedAt1 = new Date(0L);
    fail2.setFailedAt(failedAt1);
    assertThat(fail1).isNotEqualTo(fail2);
    fail1.setFailedAt(new Date(1L));
    assertThat(fail1).isNotEqualTo(fail2);
    fail1.setFailedAt(failedAt1);
    TestUtils.assertFullyEquals(fail1, fail2);
    final Date retriedAt1 = new Date(2L);
    fail2.setRetriedAt(retriedAt1);
    assertThat(fail1).isNotEqualTo(fail2);
    fail1.setRetriedAt(new Date(3L));
    assertThat(fail1).isNotEqualTo(fail2);
    fail1.setRetriedAt(retriedAt1);
    TestUtils.assertFullyEquals(fail1, fail2);
    final Throwable t1 = new Exception("BOOM");
    fail2.setThrowable(t1);
    assertThat(fail1).isNotEqualTo(fail2);
    fail1.setThrowable(t1);
    TestUtils.assertFullyEquals(fail1, fail2);
    final Throwable t2 = new Error("BOOM");
    fail2.setThrowable(t2);
    assertThat(fail1).isNotEqualTo(fail2);
    fail1.setThrowable(t2);
    TestUtils.assertFullyEquals(fail1, fail2);
    final Job payload1 = new Job();
    fail2.setPayload(payload1);
    assertThat(fail1).isNotEqualTo(fail2);
    fail1.setPayload(new Job("foo"));
    assertThat(fail1).isNotEqualTo(fail2);
    fail1.setPayload(payload1);
    TestUtils.assertFullyEquals(fail1, fail2);
    final List<String> bTrace = Arrays.asList("bTrace1", "bTrace2");
    fail2.setBacktrace(bTrace);
    assertThat(fail1).isNotEqualTo(fail2);
    fail1.setBacktrace(Arrays.asList("bTrace3"));
    assertThat(fail1).isNotEqualTo(fail2);
    fail1.setBacktrace(bTrace);
    TestUtils.assertFullyEquals(fail1, fail2);
  }
}
