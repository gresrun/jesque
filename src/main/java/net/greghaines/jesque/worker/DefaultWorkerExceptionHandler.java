package net.greghaines.jesque.worker;

import static net.greghaines.jesque.worker.WorkerRecoveryStrategy.PROCEED;
import static net.greghaines.jesque.worker.WorkerRecoveryStrategy.RECONNECT;
import static net.greghaines.jesque.worker.WorkerRecoveryStrategy.TERMINATE;

import org.codehaus.jackson.JsonProcessingException;

import redis.clients.jedis.exceptions.JedisConnectionException;

public class DefaultWorkerExceptionHandler implements WorkerExceptionHandler
{	
	public DefaultWorkerExceptionHandler(){}

	public WorkerRecoveryStrategy onException(final Worker worker,
			final Exception exception, final String curQueue)
	{
		return (exception instanceof JedisConnectionException)
				? RECONNECT 
				: (((exception instanceof JsonProcessingException) || 
					((exception instanceof InterruptedException) && !worker.isShutdown()))
						? PROCEED
						: TERMINATE);
	}
}
