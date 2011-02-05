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

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

import net.greghaines.jesque.json.ObjectMapperFactory;

import org.junit.Assert;
import org.junit.Test;

/**
 * JSON, do you know it?
 * 
 * @author Greg Haines
 */
public class TestJsonSerialization
{
	@SuppressWarnings("unchecked")
	@Test
	public void serializeJob()
	throws Exception
	{
		assertSerializeRoundTrip(new Job("foo"));
		final Job job = new Job("TestAction", new Object[]{ 1, 2.3, true, "test", Arrays.asList("inner", 4.5)});
		assertSerializeRoundTrip(job);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void serializeJobFailure()
	throws Exception
	{
		final Job job = new Job("TestAction", new Object[]{ 1, 2.3, true, "test", Arrays.asList("inner", 4.5)});
		final Exception e = new Exception("Whoopie!");
		e.fillInStackTrace();
		final JobFailure jobFailure = new JobFailure();
		assertSerializeRoundTrip(jobFailure);
		jobFailure.setPayload(job);
		jobFailure.setFailedAt(new Date());
		jobFailure.setException(e);
		jobFailure.setWorker(null);
		assertSerializeRoundTrip(jobFailure);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void serializeWorkerStatus()
	throws Exception
	{
		final Job job = new Job("TestAction", new Object[]{ 1, 2.3, true, "test", Arrays.asList("inner", 4.5)});
		final WorkerStatus workerStatus = new WorkerStatus();
		assertSerializeRoundTrip(workerStatus);
		workerStatus.setPayload(job);
		workerStatus.setQueue("foo");
		workerStatus.setRunAt(new Date());
		assertSerializeRoundTrip(workerStatus);
	}
	
	@SuppressWarnings("unchecked")
	private static <T> void assertSerializeRoundTrip(final T obj)
	throws IOException
	{
		final String json = ObjectMapperFactory.get().writeValueAsString(obj);
		final T obj2 = (T) ObjectMapperFactory.get().readValue(json, obj.getClass());
		Assert.assertEquals(obj, obj2);
	}
}
