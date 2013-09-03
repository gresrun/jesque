package net.greghaines.jesque.worker;

import static net.greghaines.jesque.worker.RecoveryStrategy.PROCEED;
import static net.greghaines.jesque.worker.RecoveryStrategy.RECONNECT;
import static net.greghaines.jesque.worker.RecoveryStrategy.TERMINATE;

import com.fasterxml.jackson.core.JsonProcessingException;

import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * An ExceptionHandler that reconnects if there is a connection exception,
 * proceeds if the exception was JSON-related or a thread interrupt and
 * terminates if the executor is shutdown.
 * 
 * @author Greg Haines
 */
public class DefaultExceptionHandler implements ExceptionHandler {

    @Override
	public RecoveryStrategy onException(final JobExecutor jobExecutor,
			final Exception exception, final String curQueue) {
		return (exception instanceof JedisConnectionException)
				? RECONNECT 
				: (((exception instanceof JsonProcessingException) || ((exception instanceof InterruptedException) && !jobExecutor.isShutdown()))
						? PROCEED
						: TERMINATE);
	}
}
