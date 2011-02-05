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

import net.greghaines.jesque.JobFailure;
import net.greghaines.jesque.utils.JesqueUtils;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;

/**
 * A custom Jackson serializer for JobFailures.
 * Needed because JobFailures uses Java-style property names and Resque does not.
 * 
 * @author Greg Haines
 */
public class JobFailureJsonSerializer extends JsonSerializer<JobFailure>
{
	@Override
	public void serialize(final JobFailure jobFailure, final JsonGenerator jgen, final SerializerProvider provider)
	throws IOException, JsonProcessingException
	{
		jgen.writeStartObject();
		jgen.writeStringField("worker", jobFailure.getWorker());
		jgen.writeFieldName("payload");
		ObjectMapperFactory.get().writeValue(jgen, jobFailure.getPayload());
		jgen.writeStringField("exception", (jobFailure.getException() == null) ? null : 
			jobFailure.getException().getClass().getName());
		jgen.writeStringField("error", (jobFailure.getException() == null) ? null : 
			jobFailure.getException().getMessage());
		jgen.writeFieldName("backtrace");
		ObjectMapperFactory.get().writeValue(jgen, (jobFailure.getException() == null) ? null : 
			JesqueUtils.createBacktrace(jobFailure.getException()));
		jgen.writeFieldName("failed_at");
		ObjectMapperFactory.get().writeValue(jgen, jobFailure.getFailedAt());
		jgen.writeEndObject();
	}
}
