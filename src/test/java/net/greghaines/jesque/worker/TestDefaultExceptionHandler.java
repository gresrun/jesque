package net.greghaines.jesque.worker;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.*;

import org.junit.Test;
import redis.clients.jedis.exceptions.JedisConnectionException;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.exc.StreamWriteException;

/**
 * Tests DefaultExceptionHandler.
 *
 * @author Greg Haines
 */
public class TestDefaultExceptionHandler {

  @Test
  public void testOnException_ConnectionEx() {
    assertThat(
            new DefaultExceptionHandler()
                .onException(null, new JedisConnectionException("foo"), null))
        .isEqualTo(RecoveryStrategy.RECONNECT);
  }

  @Test
  public void testOnException_JsonEx() {
    assertThat(
            new DefaultExceptionHandler()
                .onException(null, new StreamWriteException((JsonGenerator) null, "foo"), null))
        .isEqualTo(RecoveryStrategy.PROCEED);
  }

  @Test
  public void testOnException_Interrupted() {
    final JobExecutor jobEx = mock(JobExecutor.class);
    when(jobEx.isShutdown()).thenReturn(false);
    assertThat(
            new DefaultExceptionHandler().onException(jobEx, new InterruptedException("foo"), null))
        .isEqualTo(RecoveryStrategy.PROCEED);
    verify(jobEx).isShutdown();
  }

  @Test
  public void testOnException_InterruptedShutdown() {
    final JobExecutor jobEx = mock(JobExecutor.class);
    when(jobEx.isShutdown()).thenReturn(true);
    assertThat(
            new DefaultExceptionHandler().onException(jobEx, new InterruptedException("foo"), null))
        .isEqualTo(RecoveryStrategy.TERMINATE);
    verify(jobEx).isShutdown();
  }

  @Test
  public void testOnException_OtherEx() {
    assertThat(new DefaultExceptionHandler().onException(null, new Exception("foo"), null))
        .isEqualTo(RecoveryStrategy.TERMINATE);
  }
}
