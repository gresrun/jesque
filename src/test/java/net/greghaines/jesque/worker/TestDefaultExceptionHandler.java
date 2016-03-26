package net.greghaines.jesque.worker;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;

import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * Tests DefaultExceptionHandler.
 * @author Greg Haines
 */
public class TestDefaultExceptionHandler {

    @Test
    public void testOnException_ConnectionEx() {
        Assert.assertEquals(RecoveryStrategy.RECONNECT, 
            new DefaultExceptionHandler().onException(null, new JedisConnectionException("foo"), null));
    }

    @Test
    public void testOnException_JsonEx() {
        Assert.assertEquals(RecoveryStrategy.PROCEED, 
            new DefaultExceptionHandler().onException(null,
                    new JsonGenerationException("foo", (JsonGenerator) null), null));
    }

    @Test
    public void testOnException_Interrupted() {
        final Mockery mockCtx = new JUnit4Mockery();
        final JobExecutor jobEx = mockCtx.mock(JobExecutor.class);
        mockCtx.checking(new Expectations(){{
            allowing(jobEx).isShutdown(); will(returnValue(false));
        }});
        Assert.assertEquals(RecoveryStrategy.PROCEED, 
            new DefaultExceptionHandler().onException(jobEx, new InterruptedException("foo"), null));
        mockCtx.assertIsSatisfied();
    }

    @Test
    public void testOnException_InterruptedShutdown() {
        final Mockery mockCtx = new JUnit4Mockery();
        final JobExecutor jobEx = mockCtx.mock(JobExecutor.class);
        mockCtx.checking(new Expectations(){{
            allowing(jobEx).isShutdown(); will(returnValue(true));
        }});
        Assert.assertEquals(RecoveryStrategy.TERMINATE, 
            new DefaultExceptionHandler().onException(jobEx, new InterruptedException("foo"), null));
        mockCtx.assertIsSatisfied();
    }

    @Test
    public void testOnException_OtherEx() {
        Assert.assertEquals(RecoveryStrategy.TERMINATE, 
            new DefaultExceptionHandler().onException(null, new Exception("foo"), null));
    }
}
