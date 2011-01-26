package net.greghaines.jesque.json;

import java.io.IOException;

import net.greghaines.jesque.JobFailure;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;

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
		jgen.writeStringField("exception", jobFailure.getException());
		jgen.writeStringField("error", jobFailure.getError());
		jgen.writeFieldName("backtrace");
		ObjectMapperFactory.get().writeValue(jgen, jobFailure.getBacktrace());
		jgen.writeFieldName("failed_at");
		ObjectMapperFactory.get().writeValue(jgen, jobFailure.getFailedAt());
		jgen.writeEndObject();
	}
}
