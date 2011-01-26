package net.greghaines.jesque.json;

import java.io.IOException;


import net.greghaines.jesque.Job;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;

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