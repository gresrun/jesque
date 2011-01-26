package net.greghaines.jesque.json;

import java.io.IOException;
import java.util.Date;

import net.greghaines.jesque.Job;
import net.greghaines.jesque.WorkerStatus;

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.JsonMappingException;

public class WorkerStatusJsonDeserializer extends JsonDeserializer<WorkerStatus>
{
	@Override
	public WorkerStatus deserialize(final JsonParser jp, final DeserializationContext ctxt)
	throws IOException, JsonProcessingException
	{
		final WorkerStatus workerStatus = new WorkerStatus();
		while (jp.getCurrentToken() != JsonToken.END_OBJECT)
		{
			jp.nextToken();
			if ("run_at".equals(jp.getText()))
			{
				if (JsonToken.VALUE_STRING.equals(jp.nextToken()))
				{
					workerStatus.setRunAt(jp.readValueAs(Date.class));
				}
			}
			else if ("queue".equals(jp.getText()))
			{
				if (JsonToken.VALUE_STRING.equals(jp.nextToken()))
				{
					workerStatus.setQueue(jp.getText());
				}
			}
			else if ("payload".equals(jp.getText()))
			{
				if (JsonToken.START_OBJECT.equals(jp.nextToken()))
				{
					workerStatus.setPayload(jp.<Job>readValueAs(Job.class));
				}
			}
			else if (jp.getCurrentToken() != JsonToken.END_OBJECT)
			{
				throw new JsonMappingException("Unexpected field for WorkerStatus: " + jp.getText(), jp.getCurrentLocation());
			}
		}
		return workerStatus;
	}
}
