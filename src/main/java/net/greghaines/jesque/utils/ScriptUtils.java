/*
 * Copyright 2015 Greg Haines
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
package net.greghaines.jesque.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * ScriptUtils contains utility methods for executing Lua scripts in Redis.
 */
public final class ScriptUtils {

    /**
     * Read a script into a single-line string suitable for use in a Redis <code>EVAL</code> statement.
     * @param resourceName the name of the script resource to read
     * @return the string form of the script
     * @throws IOException if something goes wrong
     */
    public static String readScript(final String resourceName) throws IOException {
        final StringBuilder buf = new StringBuilder();
        try (final InputStream inputStream = ScriptUtils.class.getResourceAsStream(resourceName)) {
            if (inputStream == null) {
                throw new IOException("Could not find script resource: " + resourceName);
            }
            try (final BufferedReader reader = new BufferedReader(
                    new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
                String prefix = "";
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (line.length() > 0) {
                        buf.append(prefix).append(line.trim());
                        prefix = "\n";
                    }
                }
            }
        }
        return buf.toString();
    }
    
    private ScriptUtils() {
        // Utility class
    }
}
