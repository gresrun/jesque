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
				if (JsonToken.VALUE_STRING.equals(jp.nextToken()))
				{
					clazz = jp.getText();
				}
			}
			else if ("args".equals(jp.getText()))
			{
				if (JsonToken.START_ARRAY.equals(jp.nextToken()))
				{
					args = jp.readValueAs(objectArrTypeRef);
				}
			}
			else if (jp.getCurrentToken() != JsonToken.END_OBJECT)
			{
				throw new JsonMappingException("Unexpected field for Job: " + jp.getText(), jp.getCurrentLocation());
			}
		}
		return new Job(clazz, args);
	}
}