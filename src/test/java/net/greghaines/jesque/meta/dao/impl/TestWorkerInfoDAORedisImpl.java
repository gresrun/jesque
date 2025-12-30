package net.greghaines.jesque.meta.dao.impl;

import static com.google.common.truth.Truth.assertThat;
import static net.greghaines.jesque.Config.Builder.DEFAULT_NAMESPACE;
import static net.greghaines.jesque.utils.ResqueConstants.COLON;
import static net.greghaines.jesque.utils.ResqueConstants.STARTED;
import static net.greghaines.jesque.utils.ResqueConstants.WORKERS;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.text.ParseException;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.meta.WorkerInfo;
import net.greghaines.jesque.utils.CompositeDateFormat;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import redis.clients.jedis.UnifiedJedis;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class TestWorkerInfoDAORedisImpl {

  private static final String WORKERS_KEY = DEFAULT_NAMESPACE + COLON + WORKERS;

  @Mock private UnifiedJedis jedisPool;
  private WorkerInfoDAORedisImpl workerInfoDAO;

  @Before
  public void setUp() {
    this.workerInfoDAO = new WorkerInfoDAORedisImpl(Config.getDefaultConfig(), this.jedisPool);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_NullConfig() {
    new WorkerInfoDAORedisImpl(null, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_NullPool() {
    final Config config = Config.getDefaultConfig();
    new WorkerInfoDAORedisImpl(config, null);
  }

  @Test
  public void testGetWorkerCount() {
    final long workerCount = 12;
    when(this.jedisPool.scard(WORKERS_KEY)).thenReturn(workerCount);
    final long count = this.workerInfoDAO.getWorkerCount();
    assertThat(count).isEqualTo(workerCount);
  }

  @Test
  public void testRemoveWorker() {
    final String workerName = "foo";
    when(this.jedisPool.srem(WORKERS_KEY, workerName)).thenReturn(1L);
    this.workerInfoDAO.removeWorker(workerName);
    verify(this.jedisPool).srem(WORKERS_KEY, workerName);
    verify(this.jedisPool)
        .del(
            "resque:worker:foo",
            "resque:worker:foo:started",
            "resque:stat:failed:foo",
            "resque:stat:processed:foo");
  }

  @Test
  public void testIsWorkerInState_NullState() throws IOException {
    assertThat(this.workerInfoDAO.isWorkerInState("foo", null, this.jedisPool)).isTrue();
  }

  @Test
  public void testIsWorkerInState_IdleState() throws IOException {
    final String workerName = "foo";
    final String statusPayload = null;
    when(this.jedisPool.get("resque:worker:" + workerName)).thenReturn(statusPayload);
    assertThat(
            this.workerInfoDAO.isWorkerInState(workerName, WorkerInfo.State.IDLE, this.jedisPool))
        .isTrue();
  }

  @Test
  public void testIsWorkerInState_PausedState_NoStatus() throws IOException {
    final String workerName = "foo";
    final String statusPayload = null;
    when(this.jedisPool.get("resque:worker:" + workerName)).thenReturn(statusPayload);
    assertThat(
            this.workerInfoDAO.isWorkerInState(workerName, WorkerInfo.State.PAUSED, this.jedisPool))
        .isTrue();
  }

  @Test
  public void testIsWorkerInState_PausedState_Status() throws IOException {
    final String workerName = "foo";
    final String statusPayload = "{\"paused\":false}";
    when(this.jedisPool.get("resque:worker:" + workerName)).thenReturn(statusPayload);
    assertThat(
            this.workerInfoDAO.isWorkerInState(workerName, WorkerInfo.State.PAUSED, this.jedisPool))
        .isFalse();
  }

  @Test
  public void testIsWorkerInState_WorkingState_NoStatus() throws IOException {
    final String workerName = "foo";
    final String statusPayload = null;
    when(this.jedisPool.get("resque:worker:" + workerName)).thenReturn(statusPayload);
    assertThat(
            this.workerInfoDAO.isWorkerInState(
                workerName, WorkerInfo.State.WORKING, this.jedisPool))
        .isFalse();
  }

  @Test
  public void testIsWorkerInState_WorkingState_Status() throws IOException {
    final String workerName = "foo";
    final String statusPayload = "{\"paused\":false}";
    when(this.jedisPool.get("resque:worker:" + workerName)).thenReturn(statusPayload);
    assertThat(
            this.workerInfoDAO.isWorkerInState(
                workerName, WorkerInfo.State.WORKING, this.jedisPool))
        .isTrue();
  }

  @Test(expected = ParseException.class)
  public void testCreateWorker_MalformedName() throws ParseException, IOException {
    this.workerInfoDAO.createWorker("foo", this.jedisPool);
  }

  @Test
  public void testCreateWorker_Idle() throws ParseException, IOException {
    final String workerName = "foo:123:bar,baz,qux";
    final String statusPayload = null;
    final String startedStr = "2014-02-09T23:22:54.412-0400";
    final String failedStr = "2";
    final String processedStr = "6";
    when(this.jedisPool.get("resque:worker:" + workerName)).thenReturn(statusPayload);
    when(this.jedisPool.get("resque:worker:" + workerName + COLON + STARTED))
        .thenReturn(startedStr);
    when(this.jedisPool.get("resque:stat:failed:" + workerName)).thenReturn(failedStr);
    when(this.jedisPool.get("resque:stat:processed:" + workerName)).thenReturn(processedStr);
    final WorkerInfo workerInfo = this.workerInfoDAO.createWorker(workerName, this.jedisPool);
    assertThat(workerInfo.getName()).isEqualTo(workerName);
    assertThat(workerInfo.getPid()).isEqualTo("123");
    assertThat(workerInfo.getQueues()).isNotNull();
    assertThat(workerInfo.getQueues()).hasSize(3);
    assertThat(workerInfo.getQueues()).containsExactly("bar", "baz", "qux");
    assertThat(workerInfo.getState()).isEqualTo(WorkerInfo.State.IDLE);
    assertThat(workerInfo.getStatus()).isNull();
    assertThat(workerInfo.getStarted()).isEqualTo(new CompositeDateFormat().parse(startedStr));
    assertThat(workerInfo.getFailed()).isEqualTo(2L);
    assertThat(workerInfo.getProcessed()).isEqualTo(6L);
  }

  @Test
  public void testCreateWorker_Paused() throws ParseException, IOException {
    final String workerName = "foo:123:bar,baz,qux";
    final String statusPayload = "{\"paused\":true}";
    final String startedStr = null;
    final String failedStr = null;
    final String processedStr = null;
    when(this.jedisPool.get("resque:worker:" + workerName)).thenReturn(statusPayload);
    when(this.jedisPool.get("resque:worker:" + workerName + COLON + STARTED))
        .thenReturn(startedStr);
    when(this.jedisPool.get("resque:stat:failed:" + workerName)).thenReturn(failedStr);
    when(this.jedisPool.get("resque:stat:processed:" + workerName)).thenReturn(processedStr);
    final WorkerInfo workerInfo = this.workerInfoDAO.createWorker(workerName, this.jedisPool);
    assertThat(workerInfo.getName()).isEqualTo(workerName);
    assertThat(workerInfo.getPid()).isEqualTo("123");
    assertThat(workerInfo.getQueues()).isNotNull();
    assertThat(workerInfo.getQueues()).hasSize(3);
    assertThat(workerInfo.getQueues()).containsExactly("bar", "baz", "qux");
    assertThat(workerInfo.getState()).isEqualTo(WorkerInfo.State.PAUSED);
    assertThat(workerInfo.getStatus()).isNotNull();
    assertThat(workerInfo.getStatus().isPaused()).isTrue();
    assertThat(workerInfo.getStarted()).isNull();
    assertThat(workerInfo.getFailed()).isEqualTo(0L);
    assertThat(workerInfo.getProcessed()).isEqualTo(0L);
  }

  @Test
  public void testCreateWorker_Working() throws ParseException, IOException {
    final String workerName = "foo:123:bar,baz,qux";
    final String statusPayload = "{\"paused\":false}";
    final String startedStr = null;
    final String failedStr = null;
    final String processedStr = null;
    when(this.jedisPool.get("resque:worker:" + workerName)).thenReturn(statusPayload);
    when(this.jedisPool.get("resque:worker:" + workerName + COLON + STARTED))
        .thenReturn(startedStr);
    when(this.jedisPool.get("resque:stat:failed:" + workerName)).thenReturn(failedStr);
    when(this.jedisPool.get("resque:stat:processed:" + workerName)).thenReturn(processedStr);
    final WorkerInfo workerInfo = this.workerInfoDAO.createWorker(workerName, this.jedisPool);
    assertThat(workerInfo.getName()).isEqualTo(workerName);
    assertThat(workerInfo.getPid()).isEqualTo("123");
    assertThat(workerInfo.getQueues()).isNotNull();
    assertThat(workerInfo.getQueues()).hasSize(3);
    assertThat(workerInfo.getQueues()).containsExactly("bar", "baz", "qux");
    assertThat(workerInfo.getState()).isEqualTo(WorkerInfo.State.WORKING);
    assertThat(workerInfo.getStatus()).isNotNull();
    assertThat(workerInfo.getStatus().isPaused()).isFalse();
    assertThat(workerInfo.getStarted()).isNull();
    assertThat(workerInfo.getFailed()).isEqualTo(0L);
    assertThat(workerInfo.getProcessed()).isEqualTo(0L);
  }
}
