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

import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.JsonSerializer;
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
public final class ObjectMapperFactory
{
	private static final ObjectMapper mapper = new ObjectMapper();
	private static final CustomDeserializerFactory cdf = new CustomDeserializerFactory();
	private static final CustomSerializerFactory csf = new CustomSerializerFactory();

	static
	{
		mapper.configure(SerializationConfig.Feature.WRITE_DATES_AS_TIMESTAMPS, false);
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
	 * Add a custom JSON serializer for the given type.
	 * 
	 * @param <T> the type to map
	 * @param forClass the class of the type
	 * @param ser the custom serializer
	 * @param specific whether to add as a specific or generic mapping
	 */
	public static <T> void addSerializer(final Class<? extends T> forClass, 
			final JsonSerializer<T> ser, final boolean specific)
	{
		if (specific)
		{
			csf.addSpecificMapping(forClass, ser);
		}
		else
		{
			csf.addGenericMapping(forClass, ser);
		}
	}

	/**
	 * Add a custom JSON deserializer for the given type.
	 * 
	 * @param <T> the type to map
	 * @param forClass the class of the type
	 * @param deser the custom deserializer
	 */
	public static <T> void addDeserializer(final Class<T> forClass, final JsonDeserializer<? extends T> deser)
    {
		cdf.addSpecificMapping(forClass, deser);
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
