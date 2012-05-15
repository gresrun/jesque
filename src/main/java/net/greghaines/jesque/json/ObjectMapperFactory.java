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

import static net.greghaines.jesque.utils.VersionUtils.DEVELOPMENT;
import static net.greghaines.jesque.utils.VersionUtils.ERROR;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.JobFailure;
import net.greghaines.jesque.WorkerStatus;
import net.greghaines.jesque.utils.VersionUtils;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;

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
		mapper.registerModule(new SimpleModule("net.greghaines.jesque", createJacksonVersion())
			.addSerializer(Job.class, new JobJsonSerializer())
			.addDeserializer(Job.class, new JobJsonDeserializer())
			.addSerializer(JobFailure.class, new JobFailureJsonSerializer())
			.addDeserializer(JobFailure.class, new JobFailureJsonDeserializer())
			.addSerializer(WorkerStatus.class, new WorkerStatusJsonSerializer())
			.addDeserializer(WorkerStatus.class, new WorkerStatusJsonDeserializer()));
		mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
	}

	private static Version createJacksonVersion()
	{
		final Object[] versionParts = VersionUtils.getVersionParts();
		return (DEVELOPMENT.equals(versionParts[3]) || ERROR.equals(versionParts[3])) 
				? Version.unknownVersion() 
				: new Version((Integer) versionParts[0], (Integer) versionParts[1], (Integer) versionParts[2], (String) versionParts[3], "net.greghaines", "jesque");
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
