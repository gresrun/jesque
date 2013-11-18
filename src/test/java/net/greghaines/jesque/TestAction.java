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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Complicated constructor to test JSON serialization.
 * 
 * @author Greg Haines
 */
public class TestAction implements Runnable {
    
    private static final Logger log = LoggerFactory.getLogger(TestAction.class);

    private final Integer i;
    private final Double d;
    private final Boolean b;
    private final String s;
    private final List<Object> l;

    public TestAction(final Integer i, final Double d, final Boolean b, final String s, final List<Object> l) {
        this.i = i;
        this.d = d;
        this.b = b;
        this.s = s;
        this.l = l;
    }

    public void run() {
        log.info("TestAction.run() {} {} {} {} {}", new Object[] { this.i, this.d, this.b, this.s, this.l });
        try {
            Thread.sleep(100);
        } catch (Exception e) {
        }
    }
}
