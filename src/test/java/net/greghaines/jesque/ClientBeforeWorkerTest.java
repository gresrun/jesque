/*
 * Copyright 2012 Greg Haines
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package net.greghaines.jesque;

import static net.greghaines.jesque.TestUtils.createJedis;
import static net.greghaines.jesque.utils.JesqueUtils.createKey;
import static net.greghaines.jesque.utils.ResqueConstants.FAILED;
import static net.greghaines.jesque.utils.ResqueConstants.PROCESSED;
import static net.greghaines.jesque.utils.ResqueConstants.QUEUE;
import static net.greghaines.jesque.utils.ResqueConstants.STAT;

import java.util.Arrays;
import java.util.Map;
import net.greghaines.jesque.worker.MapBasedJobFactory;
import net.greghaines.jesque.worker.Worker;
import net.greghaines.jesque.worker.WorkerImpl;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;

/**
 * Github Issue #18 Reduction
 * 
 * @author Greg Haines
 */
public class ClientBeforeWorkerTest {

    private static final Config config = Config.getDefaultConfig();
    private static final String testQueue = "foo";

    @Before
    public void resetRedis() {
        TestUtils.resetRedis(config);
    }

    @Test
    public void issue18() throws Exception {
        // Enqueue the job before worker is created and started
        final Job job = new Job("TestAction",
                new Object[] {1, 2.3, true, "test", Arrays.asList("inner", 4.5)});
        TestUtils.enqueueJobs(testQueue, Arrays.asList(job), config);
        try (Jedis jedis = createJedis(config)) { // Assert that we enqueued the job
            Assert.assertEquals(1L, jedis.llen(createKey(config.getNamespace(), QUEUE, testQueue)));
        }

        // Create and start worker
        final Worker worker = new WorkerImpl(config, Arrays.asList(testQueue),
                new MapBasedJobFactory(Map.of(TestAction.class.getSimpleName(), TestAction.class)));
        final Thread workerThread = new Thread(worker);
        workerThread.start();
        try { // Wait a bit to ensure the worker had time to process the job
            Thread.sleep(500);
        } finally { // Stop the worker
            TestUtils.stopWorker(worker, workerThread);
        }

        // Assert that the job was run by the worker
        try (Jedis jedis = createJedis(config)) {
            Assert.assertEquals("1", jedis.get(createKey(config.getNamespace(), STAT, PROCESSED)));
            Assert.assertNull(jedis.get(createKey(config.getNamespace(), STAT, FAILED)));
            Assert.assertEquals(0L, jedis.llen(createKey(config.getNamespace(), QUEUE, testQueue)));
        }
    }
}
