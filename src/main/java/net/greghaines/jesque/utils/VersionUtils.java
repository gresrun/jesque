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
import java.util.regex.Pattern;

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
    private static final Pattern DOT_PATTERN = Pattern.compile("[\\.-]");

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

    /**
     * Returns an Object array of length four, where the first three elements
     * are Integers representing each of the version parts and the last, fourth,
     * element is a String that is either null, for releases, or "SNAPSHOT", for
     * snapshots. <br/>
     * <br/>
     * If this is a development build, the array is [-1, -1, -1, "DEVELOPMENT"].<br/>
     * If there is an error determining the version, the array is [-1, -1, -1, "ERROR"].
     * 
     * @return the array described
     */
    public static Object[] getVersionParts() {
        final String version = getVersion();
        final Object[] versionParts = new Object[4];
        if (DEVELOPMENT.equals(version) || ERROR.equals(version)) {
            versionParts[0] = -1;
            versionParts[1] = -1;
            versionParts[2] = -1;
            versionParts[3] = version;
        } else {
            final String[] versionStrParts = DOT_PATTERN.split(version);
            final boolean isSnapshot = (versionStrParts.length == 4);
            final int stop = isSnapshot ? versionStrParts.length - 1 : versionStrParts.length;
            for (int i = 0; i < stop; i++) {
                versionParts[i] = Integer.valueOf(versionStrParts[i]);
            }
            if (isSnapshot) {
                versionParts[versionStrParts.length - 1] = versionStrParts[versionStrParts.length - 1];
            }
        }
        return versionParts;
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
