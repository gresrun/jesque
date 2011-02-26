package net.greghaines.jesque.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingWorkerListener implements WorkerListener
{
	private static final Logger log = 
		LoggerFactory.getLogger(LoggingWorkerListener.class);

	public static final LoggingWorkerListener INSTANCE = 
		new LoggingWorkerListener();

	private LoggingWorkerListener(){} // Singlton

	public void onEvent(final WorkerEvent event, final Worker worker,
			final String queue, final net.greghaines.jesque.Job job,
			final Object runner, final Object result, final Exception ex)
	{
		if (ex == null)
		{
			log.debug("{} {} {} {} {} {} {}",
				new Object[]{event, worker, queue, job, runner, result, ex});
		}
		else
		{
			log.error(event + " " + worker + " " + queue + 
				" " + job + " " + runner + " " + result, ex);
		}
	}
}

