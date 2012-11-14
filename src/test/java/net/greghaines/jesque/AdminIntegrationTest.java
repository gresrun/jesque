package net.greghaines.jesque;

import static net.greghaines.jesque.utils.JesqueUtils.entry;
import static net.greghaines.jesque.utils.JesqueUtils.map;
import static net.greghaines.jesque.utils.JesqueUtils.set;
import net.greghaines.jesque.admin.Admin;
import net.greghaines.jesque.admin.AdminClient;
import net.greghaines.jesque.admin.AdminClientImpl;
import net.greghaines.jesque.admin.AdminImpl;
import net.greghaines.jesque.worker.Worker;
import net.greghaines.jesque.worker.WorkerImpl;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AdminIntegrationTest
{
	private static final Config config = new ConfigBuilder().build();
	private static final String testQueue = "foo";
	
	@Before
	public void resetRedis()
	{
		TestUtils.resetRedis(config);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testPauseCommand()
	{
		final Worker worker = new WorkerImpl(config, set(testQueue), map(entry("TestAction", TestAction.class)));
		final Admin admin = new AdminImpl(config);
		admin.setWorker(worker);
		
		final Thread workerThread = new Thread(worker);
		workerThread.start();
		final Thread adminThread = new Thread(admin);
		adminThread.start();

		Assert.assertFalse(worker.isPaused());
		
		try
		{
			final AdminClient adminClient = new AdminClientImpl(config);
			try
			{
				adminClient.togglePausedWorkers(true);
				try { Thread.sleep(1000L); } catch (InterruptedException ie){}
				Assert.assertTrue(worker.isPaused());

				Assert.assertFalse(worker.isShutdown());
				adminClient.shutdownWorkers(true);
				try { Thread.sleep(1000L); } catch (InterruptedException ie){}
				Assert.assertTrue(worker.isShutdown());
			}
			finally
			{
				adminClient.end();
			}
		}
		finally
		{
			TestUtils.stopWorker(admin, adminThread);
			TestUtils.stopWorker(worker, workerThread);
		}
	}
}
