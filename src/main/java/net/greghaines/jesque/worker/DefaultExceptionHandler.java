package net.greghaines.jesque.worker;

import static net.greghaines.jesque.worker.RecoveryStrategy.PROCEED;
import static net.greghaines.jesque.worker.RecoveryStrategy.RECONNECT;
import static net.greghaines.jesque.worker.RecoveryStrategy.TERMINATE;

import com.fasterxml.jackson.core.JsonProcessingException;

import redis.clients.jedis.exceptions.JedisConnectionException;

public class DefaultExceptionHandler implements ExceptionHandler
{	
	public DefaultExceptionHandler(){}

	public RecoveryStrategy onException(final JobExecutor jobExecutor,
			final Exception exception, final String curQueue)
	{
		return (exception instanceof JedisConnectionException)
				? RECONNECT 
				: (((exception instanceof JsonProcessingException) || 
					((exception instanceof InterruptedException) && !jobExecutor.isShutdown()))
						? PROCEED
						: TERMINATE);
	}
}
