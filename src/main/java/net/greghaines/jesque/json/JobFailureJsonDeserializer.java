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
import java.util.Date;
import java.util.List;

import net.greghaines.jesque.Job;
import net.greghaines.jesque.JobFailure;

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.type.TypeReference;

/**
 * A custom Jackson deserializer for JobFailures.
 * Needed because JobFailure uses Java-style property names and Resque does not.
 * 
 * @author Greg Haines
 */
public class JobFailureJsonDeserializer extends JsonDeserializer<JobFailure>
{
	private static final TypeReference<List<String>> stringListTypeRef = new TypeReference<List<String>>(){};
	
	@Override
	public JobFailure deserialize(final JsonParser jp, final DeserializationContext ctxt)
	throws IOException, JsonProcessingException
	{
		final JobFailure jobFailure = new JobFailure();
		while (jp.getCurrentToken() != JsonToken.END_OBJECT)
		{
			jp.nextToken();
			if ("worker".equals(jp.getText()))
			{
				if (JsonToken.VALUE_STRING.equals(jp.nextToken()))
				{
					jobFailure.setWorker(jp.getText());
				}
			}
			else if ("payload".equals(jp.getText()))
			{
				if (JsonToken.START_OBJECT.equals(jp.nextToken()))
				{
					jobFailure.setPayload(jp.<Job>readValueAs(Job.class));
				}
			}
			else if ("exception".equals(jp.getText()))
			{
				if (JsonToken.VALUE_STRING.equals(jp.nextToken()))
				{
					jobFailure.setException(jp.getText());
				}
			}
			else if ("error".equals(jp.getText()))
			{
				if (JsonToken.VALUE_STRING.equals(jp.nextToken()))
				{
					jobFailure.setError(jp.getText());
				}
			}
			else if ("backtrace".equals(jp.getText()))
			{
				if (JsonToken.START_ARRAY.equals(jp.nextToken()))
				{
					jobFailure.setBacktrace(jp.<List<String>>readValueAs(stringListTypeRef));
				}
			}
			else if ("failed_at".equals(jp.getText()))
			{
				if (JsonToken.VALUE_STRING.equals(jp.nextToken()))
				{
					jobFailure.setFailedAt(jp.readValueAs(Date.class));
				}
			}
			else if (jp.getCurrentToken() != JsonToken.END_OBJECT)
			{
				throw new JsonMappingException("Unexpected field for JobFailure: " + jp.getText(), jp.getCurrentLocation());
			}
		}
		return jobFailure;
	}
}
