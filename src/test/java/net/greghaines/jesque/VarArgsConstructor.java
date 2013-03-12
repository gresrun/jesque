package net.greghaines.jesque;

import static net.greghaines.jesque.TestUtils.createJedis;
import static net.greghaines.jesque.utils.JesqueUtils.createKey;
import static net.greghaines.jesque.utils.JesqueUtils.entry;
import static net.greghaines.jesque.utils.JesqueUtils.map;
import static net.greghaines.jesque.utils.ResqueConstants.FAILED;
import static net.greghaines.jesque.utils.ResqueConstants.PROCESSED;
import static net.greghaines.jesque.utils.ResqueConstants.QUEUE;
import static net.greghaines.jesque.utils.ResqueConstants.STAT;

import java.util.Arrays;

import net.greghaines.jesque.worker.Worker;
import net.greghaines.jesque.worker.WorkerImpl;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;

public class VarArgsConstructor {
	private static final Config config = new ConfigBuilder().build();
	private static final String testQueue = "foo";
	
	@Before
	public void resetRedis()
	{
		TestUtils.resetRedis(config);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void consumeGenericConstructorJob()
	throws Exception
	{
		// Enqueue the job before worker is created and started
		final Job job = new Job("GenericWorker", new Object[]{1, 2.3, "new value", Arrays.asList("inner", 4.5)});
		
		TestUtils.enqueueJobs(testQueue, Arrays.asList(job), config);
		Jedis jedis = createJedis(config);
		try
		{ // Assert that we enqueued the job
			Assert.assertEquals(Long.valueOf(1), jedis.llen(createKey(config.getNamespace(), QUEUE, testQueue)));
		}
		finally
		{
			jedis.quit();
		}
		
		// Create and start worker
		final Worker worker = new WorkerImpl(config, Arrays.asList(testQueue), map(entry("GenericWorker", GenericWorker.class)));
		final Thread workerThread = new Thread(worker);
		workerThread.start();
		try
		{ // Wait a bit to ensure the worker had time to process the job
			Thread.sleep(500);
		}
		finally
		{ // Stop the worker
			TestUtils.stopWorker(worker, workerThread);
		}
		
		// Assert that the job was run by the worker
		jedis = createJedis(config);
		try
		{
			Assert.assertEquals("1", jedis.get(createKey(config.getNamespace(), STAT, PROCESSED)));
			Assert.assertNull(jedis.get(createKey(config.getNamespace(), STAT, FAILED)));
			Assert.assertEquals(Long.valueOf(0), jedis.llen(createKey(config.getNamespace(), QUEUE, testQueue)));
		}
		finally
		{
			jedis.quit();
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test	
	public void consumeSpecificConstructorJob()
	throws Exception
	{
		// Enqueue the job before worker is created and started
		final Job job = new Job("GenericWorker", new Object[]{1, 2.3, "value"});
		
		TestUtils.enqueueJobs(testQueue, Arrays.asList(job), config);
		Jedis jedis = createJedis(config);
		try
		{ // Assert that we enqueued the job
			Assert.assertEquals(Long.valueOf(1), jedis.llen(createKey(config.getNamespace(), QUEUE, testQueue)));
		}
		finally
		{
			jedis.quit();
		}
		
		// Create and start worker
		final Worker worker = new WorkerImpl(config, Arrays.asList(testQueue), map(entry("GenericWorker", GenericWorker.class)));
		final Thread workerThread = new Thread(worker);
		workerThread.start();
		try
		{ // Wait a bit to ensure the worker had time to process the job
			Thread.sleep(500);
		}
		finally
		{ // Stop the worker
			TestUtils.stopWorker(worker, workerThread);
		}
		
		// Assert that the job was run by the worker
		jedis = createJedis(config);
		try
		{
			Assert.assertEquals("1", jedis.get(createKey(config.getNamespace(), STAT, PROCESSED)));
			Assert.assertNull(jedis.get(createKey(config.getNamespace(), STAT, FAILED)));
			Assert.assertEquals(Long.valueOf(0), jedis.llen(createKey(config.getNamespace(), QUEUE, testQueue)));
		}
		finally
		{
			jedis.quit();
		}
	}	
}
