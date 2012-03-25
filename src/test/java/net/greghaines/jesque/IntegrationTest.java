/*
 * Copyright 2011 Greg Haines
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.greghaines.jesque;

import static net.greghaines.jesque.TestUtils.createJedis;
import static net.greghaines.jesque.utils.JesqueUtils.createKey;
import static net.greghaines.jesque.utils.JesqueUtils.entry;
import static net.greghaines.jesque.utils.JesqueUtils.map;
import static net.greghaines.jesque.utils.ResqueConstants.FAILED;
import static net.greghaines.jesque.utils.ResqueConstants.PROCESSED;
import static net.greghaines.jesque.utils.ResqueConstants.STAT;
import static net.greghaines.jesque.worker.WorkerEvent.JOB_FAILURE;
import static net.greghaines.jesque.worker.WorkerEvent.JOB_PROCESS;
import static net.greghaines.jesque.worker.WorkerEvent.JOB_SUCCESS;
import static net.greghaines.jesque.worker.WorkerEvent.WORKER_ERROR;
import static net.greghaines.jesque.worker.WorkerEvent.WORKER_POLL;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import net.greghaines.jesque.client.ClientImpl;
import net.greghaines.jesque.worker.UnpermittedJobException;
import net.greghaines.jesque.worker.Worker;
import net.greghaines.jesque.worker.WorkerEvent;
import net.greghaines.jesque.worker.WorkerImpl;
import net.greghaines.jesque.worker.WorkerListener;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

/**
 * A.K.A. - The Whole Enchillada
 * 
 * @author Greg Haines
 */
public class IntegrationTest
{
	private static final Logger log = LoggerFactory.getLogger(IntegrationTest.class);
	private static final Config config = new ConfigBuilder().build();
	private static final String testQueue = "foo";
	
	@Before
	public void resetRedis()
	throws Exception
	{
		final Jedis jedis = createJedis(config);
		try
		{
			log.info("Resetting Redis for next test...");
			jedis.flushDB();
		}
		finally
		{
			jedis.quit();
		}
	}
	
	@Test
	public void jobSuccess()
	throws Exception
	{
		log.info("Running jobSuccess()...");
		assertSuccess(null, false);
	}
	
	@Test
	public void jobSuccessUseHeadQueue()
	throws Exception
	{
		log.info("Running jobSuccess()...");
		assertSuccess(null, true);
	}

	
	@Test
	public void jobFailure()
	throws Exception
	{
		log.info("Running jobFailure()...");
		assertFailure(null, false);
	}
	
	@Test
	public void jobFailureUseHeadQueue()
	throws Exception
	{
		log.info("Running jobFailure()...");
		assertFailure(null, true);
	}

	
	@Test
	public void jobMixed()
	throws Exception
	{
		log.info("Running jobMixed()...");
		assertMixed(null);
	}
	
	@Test
	public void successInSpiteOfListenerFailPoll()
	{
		log.info("Running successInSpiteOfListenerFailPoll()...");
		assertSuccess(new FailingWorkerListener(), false, WORKER_POLL);
	}
	
	@Test
	public void successInSpiteOfListenerFailJob()
	{
		log.info("Running successInSpiteOfListenerFailJob()...");
		assertSuccess(new FailingWorkerListener(), false, JOB_PROCESS);
	}
	
	@Test
	public void successInSpiteOfListenerFailSuccess()
	{
		log.info("Running successInSpiteOfListenerFailSuccess()...");
		assertSuccess(new FailingWorkerListener(), false, JOB_SUCCESS);
	}
	
	@Test
	public void successInSpiteOfListenerFailAll()
	{
		log.info("Running successInSpiteOfListenerFailAll()...");
		assertSuccess(new FailingWorkerListener(), false, WorkerEvent.values());
	}
	
	@Test
	public void failureInSpiteOfListenerFailError()
	{
		log.info("Running failureInSpiteOfListenerFailError()...");
		assertFailure(new FailingWorkerListener(), false, WORKER_ERROR);
	}
	
	@Test
	public void failureInSpiteOfListenerFailAll()
	{
		log.info("Running failureInSpiteOfListenerFailAll()...");
		assertFailure(new FailingWorkerListener(), false, WorkerEvent.values());
	}
	
	@Test
	public void mixedInSpiteOfListenerFailAll()
	{
		log.info("Running mixedInSpiteOfListenerFailAll()...");
		assertMixed(new FailingWorkerListener(), WorkerEvent.values());
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void unpermittedJob()
	{
		final Job job = new Job("TestAction", new Object[]{ 1, 2.3, true, "test", Arrays.asList("inner", 4.5)});
		final AtomicBoolean didFailWithUnpermittedJob = new AtomicBoolean(false);
		doWork(Arrays.asList(job), map(entry("FailAction", FailAction.class)), new WorkerListener()
		{
			public void onEvent(final WorkerEvent event, final Worker worker, final String queue,
					final Job job, final Object runner, final Object result, final Exception ex)
			{
				if (JOB_FAILURE.equals(event) && (ex instanceof UnpermittedJobException))
				{
					didFailWithUnpermittedJob.set(true);
				}
			}
		}, JOB_FAILURE);
		
		final Jedis jedis = createJedis(config);
		try
		{
			Assert.assertTrue(didFailWithUnpermittedJob.get());
			Assert.assertEquals("1", jedis.get(createKey(config.getNamespace(), STAT, FAILED)));
			Assert.assertNull(jedis.get(createKey(config.getNamespace(), STAT, PROCESSED)));
		}
		finally
		{
			jedis.quit();
		}
	}
	


	
	@SuppressWarnings("unchecked")
	private static void assertSuccess(final WorkerListener listener, boolean isHeadQueue, final WorkerEvent... events)
	{
		final Job job = new Job("TestAction", new Object[]{ 1, 2.3, true, "test", Arrays.asList("inner", 4.5)});
		
		doWork(Arrays.asList(job), map(entry("TestAction", TestAction.class)), listener, isHeadQueue, events);

		final Jedis jedis = createJedis(config);
		try
		{
			Assert.assertEquals("1", jedis.get(createKey(config.getNamespace(), STAT, PROCESSED)));
			Assert.assertNull(jedis.get(createKey(config.getNamespace(), STAT, FAILED)));
		}
		finally
		{
			jedis.quit();
		}
	}


	
	@SuppressWarnings("unchecked")
	private static void assertFailure(final WorkerListener listener, boolean isHeadQueue, final WorkerEvent... events)
	{
		final Job job = new Job("FailAction");
		
		doWork(Arrays.asList(job), map(entry("FailAction", FailAction.class)), listener, isHeadQueue, events);
		
		final Jedis jedis = createJedis(config);
		try
		{
			Assert.assertEquals("1", jedis.get(createKey(config.getNamespace(), STAT, FAILED)));
			Assert.assertNull(jedis.get(createKey(config.getNamespace(), STAT, PROCESSED)));
		}
		finally
		{
			jedis.quit();
		}
	}
	
	@SuppressWarnings("unchecked")
	private static void assertMixed(final WorkerListener listener, final WorkerEvent... events)
	{
		final Job job1 = new Job("FailAction");
		final Job job2 = new Job("TestAction", new Object[]{ 1, 2.3, true, "test", Arrays.asList("inner", 4.5)});
		final Job job3 = new Job("FailAction");
		final Job job4 = new Job("TestAction", new Object[]{ 1, 2.3, true, "test", Arrays.asList("inner", 4.5)});
		
		doWork(Arrays.asList(job1, job2, job3, job4), map(entry("FailAction", FailAction.class), entry("TestAction", TestAction.class)), listener, events);
		
		final Jedis jedis = createJedis(config);
		try
		{
			Assert.assertEquals("2", jedis.get(createKey(config.getNamespace(), STAT, FAILED)));
			Assert.assertEquals("2", jedis.get(createKey(config.getNamespace(), STAT, PROCESSED)));
		}
		finally
		{
			jedis.quit();
		}
	}
	
	
	private static void doWork(final List<Job> jobs, final Map<String,? extends Class<? extends Runnable>> jobTypes,
			final WorkerListener listener, final WorkerEvent... events) {
		doWork(jobs, jobTypes, listener, false, events);
	}
	
	private static void doWork(final List<Job> jobs, final Map<String,? extends Class<? extends Runnable>> jobTypes,
			final WorkerListener listener, boolean isHeadQueue, final WorkerEvent... events)
	{
		final Worker worker = new WorkerImpl(config, Arrays.asList(testQueue), jobTypes);
		if (listener != null && events.length > 0)
		{
			worker.addListener(listener, events);
		}
		final Thread workerThread = new Thread(worker);
		workerThread.start();
		try
		{
			if(isHeadQueue) TestUtils.headQueueJobs(testQueue, jobs, config);
			else 			TestUtils.enqueueJobs(testQueue, jobs, config);
		}
		finally
		{
			TestUtils.stopWorker(worker, workerThread);
		}
	}
	
	private static class FailingWorkerListener implements WorkerListener
	{
		public void onEvent(final WorkerEvent event, final Worker worker, final String queue,
				final Job job, final Object runner, final Object result, final Exception ex)
		{
			throw new RuntimeException("Listener FAIL");
		}
	}
	
	@Test
	public void acquireLockSuccess(){
        log.info("Running acquireLockSuccess()");
	    ClientImpl client = new ClientImpl(config);
	    Assert.assertTrue("Failed to acquire the expected lock.", client.acquireLock("systemLockA", "me", 10));
	    Assert.assertTrue("Failed to acquire the expected lock.", client.acquireLock("systemLockA", "me", 10));
	    Assert.assertTrue("Failed to acquire the expected lock.", client.acquireLock("systemLockA", "me", 10));
	}
	
	@Test
	public void acquireLockFail(){
        log.info("Running acquireLockFail()");
	    ClientImpl client = new ClientImpl(config);
	    Assert.assertTrue("Failed to acquire the expected lock.", client.acquireLock("systemLockA", "pete", 10000));
	    Assert.assertFalse("Acquired lock that should have been in use.", client.acquireLock("systemLockA", "george", 10));
	    Assert.assertTrue("Failed to acquire the expected lock.", client.acquireLock("systemLockA", "pete", 10000));
	}
}
