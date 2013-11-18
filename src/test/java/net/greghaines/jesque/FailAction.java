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
package net.greghaines.jesque;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Born to fail.
 * 
 * @author Greg Haines
 */
public class FailAction implements Runnable {
    
    private static final Logger log = LoggerFactory.getLogger(FailAction.class);

    public FailAction() {}

    public void run() {
        log.info("FailAction.run()");
        try {
            throw new IOException("poof.");
        } catch (IOException ioe) {
            throw new RuntimeException("BOOM!", ioe);
        }
    }
}
