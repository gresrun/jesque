package net.greghaines.jesque;

import static net.greghaines.jesque.utils.JesqueUtils.entry;
import static net.greghaines.jesque.utils.JesqueUtils.map;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import net.greghaines.jesque.worker.Worker;
import net.greghaines.jesque.worker.WorkerEvent;
import net.greghaines.jesque.worker.WorkerImpl;
import net.greghaines.jesque.worker.WorkerListener;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LongRunningTest
{
	private static final Logger log = LoggerFactory.getLogger(LongRunningTest.class);
	private static final Config config = new ConfigBuilder().build();
	
	private static void sleepTight(final long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Before
	public void resetRedis()
	throws Exception
	{
		TestUtils.resetRedis(config);
	}
	
	@Test
	public void dummy(){} // Makes JUnit happy that there's at least one test if the others are commented out
	
	@SuppressWarnings("unchecked")
//	@Test
	public void issue28()
	throws InterruptedException
	{
		TestUtils.enqueueJobs("longRunning", Arrays.asList(new Job("LongRunningAction")), config);
		final Worker worker2 = new WorkerImpl(config, Arrays.asList("longRunning"), 
			map(entry("LongRunningAction", LongRunningAction.class)));
		final AtomicBoolean succesCallbackInvokedRef = new AtomicBoolean(false);
		worker2.addListener(new WorkerListener() {
			@Override
			public void onEvent(final WorkerEvent event, final Worker worker, final String queue,
					final Job job, final Object runner, final Object result, final Exception ex) {
				succesCallbackInvokedRef.set(true);
				log.info("SUCCCES- {}", job);
			}
		}, WorkerEvent.JOB_SUCCESS);
		final Thread workerThread2 = new Thread(worker2);
		workerThread2.start();
		sleepTight(1000);
		worker2.end(false);
		workerThread2.join();
		Assert.assertTrue("Success callback should have been called", succesCallbackInvokedRef.get());
	}
	
	public static class LongRunningAction implements Runnable
	{
		public LongRunningAction(){}

		public void run()
		{
			sleepTight(10*60*1000); // Sleep for 10 minutes
		}
	}
}
