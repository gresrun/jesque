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
package net.greghaines.jesque.json;

import net.greghaines.jesque.Job;
import net.greghaines.jesque.JobFailure;
import net.greghaines.jesque.WorkerStatus;
import net.greghaines.jesque.utils.VersionUtils;

import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.module.SimpleModule;

/**
 * A helper that creates a fully-configured singleton ObjectMapper.
 * 
 * @author Greg Haines
 */
public final class ObjectMapperFactory
{
	private static final ObjectMapper mapper = new ObjectMapper();

	static
	{
		final SimpleModule jesqueModule = new SimpleModule("net.greghaines.jesque", createJacksonVersion());
		jesqueModule.addSerializer(Job.class, new JobJsonSerializer());
		jesqueModule.addDeserializer(Job.class, new JobJsonDeserializer());
		jesqueModule.addSerializer(JobFailure.class, new JobFailureJsonSerializer());
		jesqueModule.addDeserializer(JobFailure.class, new JobFailureJsonDeserializer());
		jesqueModule.addSerializer(WorkerStatus.class, new WorkerStatusJsonSerializer());
		jesqueModule.addDeserializer(WorkerStatus.class, new WorkerStatusJsonDeserializer());
		mapper.registerModule(jesqueModule);
		mapper.configure(SerializationConfig.Feature.WRITE_DATES_AS_TIMESTAMPS, false);
	}

	private static Version createJacksonVersion()
	{
		final Object[] versionParts = VersionUtils.getVersionParts();
		return new Version((Integer) versionParts[0], (Integer) versionParts[1], (Integer) versionParts[2], (String) versionParts[3]);
	}
	
	/**
	 * @return a fully-configured ObjectMapper
	 */
	public static ObjectMapper get()
	{
		return mapper;
	}
	
	private ObjectMapperFactory(){} // Utility class
}
