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

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.type.TypeReference;

/**
 * A custom Jackson deserializer for Jobs.
 * Needed because Job uses Java-style property names and Resque does not.
 * 
 * @author Greg Haines
 */
public class JobJsonDeserializer extends JsonDeserializer<Job>
{
	private static final TypeReference<Object[]> objectArrTypeRef = new TypeReference<Object[]>(){};
	
	@Override
	public Job deserialize(final JsonParser jp, final DeserializationContext ctxt)
	throws IOException, JsonProcessingException
	{
		String clazz = null;
		Object[] args = null;
		while (jp.getCurrentToken() != JsonToken.END_OBJECT)
		{
			jp.nextToken();
			if ("class".equals(jp.getText()))
			{
				jp.nextToken();
				clazz = jp.readValueAs(String.class);
			}
			else if ("args".equals(jp.getText()))
			{
				jp.nextToken();
				args = jp.readValueAs(objectArrTypeRef);
			}
			else if (jp.getCurrentToken() != JsonToken.END_OBJECT)
			{
				throw new JsonMappingException("Unexpected field for Job: " + jp.getText(), jp.getCurrentLocation());
			}
		}
		return new Job(clazz, args);
	}
}
