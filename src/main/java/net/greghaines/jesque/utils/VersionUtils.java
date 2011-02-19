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
public final class VersionUtils
{
	public static final String DEVELOPMENT = "DEVELOPMENT";
	public static final String ERROR = "ERROR";
	
	private static final Logger log = LoggerFactory.getLogger(VersionUtils.class);
	private static final String pomPropertiesResName = "/META-INF/maven/net.greghaines/jesque/pom.properties";
	private static final String versionPropName = "version";
	private static final AtomicReference<String> versionRef = new AtomicReference<String>(null);
	
	/**
	 * @return the current version of this software
	 */
	public static String getVersion()
	{
		String version = versionRef.get();
		if (version == null)
		{
			versionRef.set(readVersion());
			version = versionRef.get();
		}
		return version;
	}

	private static String readVersion()
	{
		String version = DEVELOPMENT;
		final InputStream stream = VersionUtils.class.getResourceAsStream(pomPropertiesResName);
		if (stream != null)
		{
			try
			{
				final Properties props = new Properties();
				props.load(stream);
				version = (String) props.get(versionPropName);
			}
			catch (Exception e)
			{
				log.warn("Could not determine version from POM properties", e);
				version = ERROR;
			}
			finally
			{
				try { stream.close(); } catch (Exception e){}
			}
		}
		return version;
	}
	
	private VersionUtils(){} // Utiltity class
}
