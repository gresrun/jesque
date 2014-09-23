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
package net.greghaines.jesque.utils;

import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Grabs the version number from the Maven metadata.
 * 
 * @author Greg Haines
 */
public final class VersionUtils {
    
    /**
     * String for non-released versions.
     */
    public static final String DEVELOPMENT = "DEVELOPMENT";
    /**
     * String shown if there was an error determining the version.
     */
    public static final String ERROR = "ERROR";

    private static final Logger LOG = LoggerFactory.getLogger(VersionUtils.class);
    private static final String POM_PROPERTIES_RES_NAME = "/META-INF/maven/net.greghaines/jesque/pom.properties";
    private static final String VERSION_PROP_NAME = "version";
    private static final AtomicReference<String> VERSION_REF = new AtomicReference<String>(null);

    /**
     * @return the current version of this software
     */
    public static String getVersion() {
        String version = VERSION_REF.get();
        if (version == null) {
            VERSION_REF.set(readVersion());
            version = VERSION_REF.get();
        }
        return version;
    }

    private static String readVersion() {
        String version = DEVELOPMENT;
        final InputStream stream = VersionUtils.class.getResourceAsStream(POM_PROPERTIES_RES_NAME);
        if (stream != null) {
            try {
                final Properties props = new Properties();
                props.load(stream);
                version = (String) props.get(VERSION_PROP_NAME);
            } catch (Exception e) {
                LOG.warn("Could not determine version from POM properties", e);
                version = ERROR;
            } finally {
                try {
                    stream.close();
                } catch (Exception e) {
                    LOG.debug("Error closing pom properties resource stream");
                }
            }
        }
        return version;
    }

    private VersionUtils() {
        // Utility class
    }
}
