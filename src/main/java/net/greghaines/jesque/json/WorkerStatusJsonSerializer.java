package net.greghaines.jesque.json;

import java.io.IOException;

import net.greghaines.jesque.WorkerStatus;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;

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
