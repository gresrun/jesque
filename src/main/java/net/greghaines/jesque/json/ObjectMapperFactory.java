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

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.deser.CustomDeserializerFactory;
import org.codehaus.jackson.map.deser.StdDeserializerProvider;
import org.codehaus.jackson.map.ser.CustomSerializerFactory;

/**
 * A helper that creates a fully-configured singleton ObjectMapper.
 * 
 * @author Greg Haines
 */
public class ObjectMapperFactory
{
	private static final ObjectMapper mapper = new ObjectMapper();

	static
	{
		mapper.configure(SerializationConfig.Feature.WRITE_DATES_AS_TIMESTAMPS, false);
		final CustomDeserializerFactory cdf = new CustomDeserializerFactory();
		final CustomSerializerFactory csf = new CustomSerializerFactory();
		mapper.setDeserializerProvider(new StdDeserializerProvider(cdf));
		mapper.setSerializerFactory(csf);
		cdf.addSpecificMapping(Job.class, new JobJsonDeserializer());
		csf.addSpecificMapping(Job.class, new JobJsonSerializer());
		cdf.addSpecificMapping(JobFailure.class, new JobFailureJsonDeserializer());
		csf.addSpecificMapping(JobFailure.class, new JobFailureJsonSerializer());
		cdf.addSpecificMapping(WorkerStatus.class, new WorkerStatusJsonDeserializer());
		csf.addSpecificMapping(WorkerStatus.class, new WorkerStatusJsonSerializer());
	}
	
	/**
	 * @return a fully-configured ObjectMapper
	 */
	public static ObjectMapper get()
	{
		return mapper;
	}
}
