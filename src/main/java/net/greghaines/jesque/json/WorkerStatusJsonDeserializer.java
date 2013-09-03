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

import net.greghaines.jesque.Job;
import net.greghaines.jesque.WorkerStatus;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;

/**
 * A custom Jackson deserializer for WorkerStatuses. Needed because WorkerStatus
 * uses Java-style property names and Resque does not.
 * 
 * @author Greg Haines
 */
public class WorkerStatusJsonDeserializer extends JsonDeserializer<WorkerStatus> {

    @Override
    public WorkerStatus deserialize(final JsonParser jp, final DeserializationContext ctxt) throws IOException,
            JsonProcessingException {
        final WorkerStatus workerStatus = new WorkerStatus();
        while (jp.getCurrentToken() != JsonToken.END_OBJECT) {
            jp.nextToken();
            if ("run_at".equals(jp.getText())) {
                jp.nextToken();
                workerStatus.setRunAt(jp.readValueAs(Date.class));
            } else if ("queue".equals(jp.getText())) {
                jp.nextToken();
                workerStatus.setQueue(jp.readValueAs(String.class));
            } else if ("payload".equals(jp.getText())) {
                jp.nextToken();
                workerStatus.setPayload(jp.readValueAs(Job.class));
            } else if ("paused".equals(jp.getText())) {
                jp.nextToken();
                workerStatus.setPaused(jp.readValueAs(Boolean.class));
            } else if (jp.getCurrentToken() != JsonToken.END_OBJECT) {
                throw new JsonMappingException("Unexpected field for WorkerStatus: " + jp.getText(),
                        jp.getCurrentLocation());
            }
        }
        return workerStatus;
    }
}
