package net.greghaines.jesque.worker;

import static org.mockito.Mockito.*;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * Tests DefaultExceptionHandler.
 *
 * @author Greg Haines
 */
public class TestDefaultExceptionHandler {

  @Test
  public void testOnException_ConnectionEx() {
    Assert.assertEquals(
        RecoveryStrategy.RECONNECT,
        new DefaultExceptionHandler().onException(null, new JedisConnectionException("foo"), null));
  }

  @Test
  public void testOnException_JsonEx() {
    Assert.assertEquals(
        RecoveryStrategy.PROCEED,
        new DefaultExceptionHandler()
            .onException(null, new JsonGenerationException("foo", (JsonGenerator) null), null));
  }

  @Test
  public void testOnException_Interrupted() {
    final JobExecutor jobEx = mock(JobExecutor.class);
    when(jobEx.isShutdown()).thenReturn(false);
    Assert.assertEquals(
        RecoveryStrategy.PROCEED,
        new DefaultExceptionHandler().onException(jobEx, new InterruptedException("foo"), null));
    verify(jobEx).isShutdown();
  }

  @Test
  public void testOnException_InterruptedShutdown() {
    final JobExecutor jobEx = mock(JobExecutor.class);
    when(jobEx.isShutdown()).thenReturn(true);
    Assert.assertEquals(
        RecoveryStrategy.TERMINATE,
        new DefaultExceptionHandler().onException(jobEx, new InterruptedException("foo"), null));
    verify(jobEx).isShutdown();
  }

  @Test
  public void testOnException_OtherEx() {
    Assert.assertEquals(
        RecoveryStrategy.TERMINATE,
        new DefaultExceptionHandler().onException(null, new Exception("foo"), null));
  }
}
