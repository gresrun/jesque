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

import net.greghaines.jesque.worker.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static net.greghaines.jesque.TestUtils.createJedis;
import static net.greghaines.jesque.utils.JesqueUtils.*;
import static net.greghaines.jesque.utils.ResqueConstants.*;

/**
 * @author Noam Y. Tenne
 */
public class CustomizableActionIntegrationTest
{
	private static final Config config = new ConfigBuilder().build();
	private static final String testQueue = "foo";
	
	@Before
	public void resetRedis()
	{
		TestUtils.resetRedis(config);
	}

    @Test
    public void testFailingCustomizableAction()
    {
		final Job job = new Job("CustomizableAction");
		
		doWork(Arrays.asList(job), map(entry("CustomizableAction", CustomizableAction.class)), null);
		
		final Jedis jedis = createJedis(config);
		try
		{
			Assert.assertNull(jedis.get(createKey(config.getNamespace(), STAT, PROCESSED)));
			Assert.assertEquals("1", jedis.get(createKey(config.getNamespace(), STAT, FAILED)));
		}
		finally
		{
			jedis.quit();
		}
	}

    @Test
    public void testSucceedingCustomizableAction()
    {
		final Job job = new Job("CustomizableAction");

        ActionCustomizer customizer = new ActionCustomizer()
        {
            @Override
            public void customize(Object actionInstance)
            {
                ((CustomizableAction) actionInstance).setFailAction(false);
            }
        };

        doWork(Arrays.asList(job), map(entry("CustomizableAction", CustomizableAction.class)), customizer);

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

	private static void doWork(final List<Job> jobs, final Map<String,? extends Class<? extends Runnable>> jobTypes,
			ActionCustomizer customizer)
	{
		final Worker worker = new WorkerImpl(config, Arrays.asList(testQueue), jobTypes, customizer);
		final Thread workerThread = new Thread(worker);
		workerThread.start();
		try
		{
			TestUtils.enqueueJobs(testQueue, jobs, config);
		}
		finally
		{
			TestUtils.stopWorker(worker, workerThread);
		}
	}
}
