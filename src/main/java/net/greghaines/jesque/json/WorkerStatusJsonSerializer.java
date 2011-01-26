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

import net.greghaines.jesque.WorkerStatus;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;

/**
 * A custom Jackson serializer for WorkerStatuses.
 * Needed because WorkerStatus uses Java-style property names and Resque does not.
 * 
 * @author Greg Haines
 */
public class WorkerStatusJsonSerializer extends JsonSerializer<WorkerStatus>
{
	@Override
	public void serialize(final WorkerStatus workerStatus, final JsonGenerator jgen, final SerializerProvider provider)
	throws IOException, JsonProcessingException
	{
		jgen.writeStartObject();
		jgen.writeFieldName("run_at");
		ObjectMapperFactory.get().writeValue(jgen, workerStatus.getRunAt());
		jgen.writeStringField("queue", workerStatus.getQueue());
		jgen.writeFieldName("payload");
		ObjectMapperFactory.get().writeValue(jgen, workerStatus.getPayload());
		jgen.writeEndObject();
	}
}
