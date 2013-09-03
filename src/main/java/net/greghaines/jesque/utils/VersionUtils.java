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
    
    public static final String SNAPSHOT = "SNAPSHOT";
    public static final String DEVELOPMENT = "DEVELOPMENT";
    public static final String ERROR = "ERROR";

    private static final Logger log = LoggerFactory.getLogger(VersionUtils.class);
    private static final String pomPropertiesResName = "/META-INF/maven/net.greghaines/jesque/pom.properties";
    private static final String versionPropName = "version";
    private static final AtomicReference<String> versionRef = new AtomicReference<String>(null);
    private static final Pattern dotPattern = Pattern.compile("[\\.-]");

    /**
     * @return the current version of this software
     */
    public static String getVersion() {
        String version = versionRef.get();
        if (version == null) {
            versionRef.set(readVersion());
            version = versionRef.get();
        }
        return version;
    }

    /**
     * Returns an Object array of length four, where the first three elements
     * are Integers representing each of the version parts and the last, fourth,
     * element is a String that is either null, for releases, or "SNAPSHOT", for
     * snapshots. <br/>
     * <br/>
     * If this is a developement build, the array is [-1, -1, -1,
     * "DEVELOPMENT"].<br/>
     * If there is an error determining the version, the array is [-1, -1, -1,
     * "ERROR"].
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
            final String[] versionStrParts = dotPattern.split(version);
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
        final InputStream stream = VersionUtils.class.getResourceAsStream(pomPropertiesResName);
        if (stream != null) {
            try {
                final Properties props = new Properties();
                props.load(stream);
                version = (String) props.get(versionPropName);
            } catch (Exception e) {
                log.warn("Could not determine version from POM properties", e);
                version = ERROR;
            } finally {
                try {
                    stream.close();
                } catch (Exception e) {
                }
            }
        }
        return version;
    }

    private VersionUtils() {
        // Utility class
    }
}
