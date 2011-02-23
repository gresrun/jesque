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
import net.greghaines.jesque.utils.JesqueUtils;

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
		String exception = null;
		String error = null;
		List<String> backtrace = null;
		while (jp.getCurrentToken() != JsonToken.END_OBJECT)
		{
			jp.nextToken();
			if ("worker".equals(jp.getText()))
			{
				jp.nextToken();
				jobFailure.setWorker(jp.readValueAs(String.class));
			}
			else if ("payload".equals(jp.getText()))
			{
				jp.nextToken();
				
				jobFailure.setPayload(jp.readValueAs(Job.class));
			}
			else if ("exception".equals(jp.getText()))
			{
				jp.nextToken();
				exception = jp.readValueAs(String.class);
			}
			else if ("error".equals(jp.getText()))
			{
				jp.nextToken();
				error = jp.readValueAs(String.class);
			}
			else if ("backtrace".equals(jp.getText()))
			{
				jp.nextToken();
				backtrace = jp.<List<String>>readValueAs(stringListTypeRef);
			}
			else if ("failed_at".equals(jp.getText()))
			{
				jp.nextToken();
				jobFailure.setFailedAt(jp.readValueAs(Date.class));
			}
			else if (jp.getCurrentToken() != JsonToken.END_OBJECT)
			{
				throw new JsonMappingException("Unexpected field for JobFailure: " + jp.getText(), jp.getCurrentLocation());
			}
		}
		if (exception != null)
		{
			try
			{
				jobFailure.setException(JesqueUtils.recreateThrowable(exception, error, backtrace));
			}
			catch (Exception e)
			{
				throw new JsonMappingException("Unable to recreate exception for JobFailure", e);
			}
		}
		return jobFailure;
	}
}
