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

import java.io.IOException;


import net.greghaines.jesque.Job;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;

/**
 * A custom Jackson serializer for Jobs.
 * Needed because Job uses Java-style property names and Resque does not.
 * 
 * @author Greg Haines
 */
public class JobJsonSerializer extends JsonSerializer<Job>
{
	@Override
	public void serialize(final Job job, final JsonGenerator jgen, final SerializerProvider provider)
	throws IOException, JsonProcessingException
	{
		jgen.writeStartObject();
		jgen.writeStringField("class", job.getClassName());
		jgen.writeFieldName("args");
		ObjectMapperFactory.get().writeValue(jgen, job.getArgs());
		jgen.writeEndObject();
	}
}
