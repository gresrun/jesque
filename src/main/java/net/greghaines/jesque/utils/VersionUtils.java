package net.greghaines.jesque.utils;

import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class VersionUtils
{
	private static final Logger log = LoggerFactory.getLogger(VersionUtils.class);
	private static final String pomPropertiesResName = "/META-INF/maven/net.greghaines/jesque/pom.properties";
	private static final AtomicReference<String> version = new AtomicReference<String>(null);
	
	/**
	 * Get the current version of this software.
	 * @return
	 */
	public static String getVersion()
	{
		String v = version.get();
		if (v == null)
		{
			version.compareAndSet(null, readVersion());
			v = version.get();
		}
		return v;
	}

	private static String readVersion()
	{
		String version = "DEVELOPMENT";
		InputStream stream = VersionUtils.class.getResourceAsStream(pomPropertiesResName);
		if (stream != null)
		{
			try
			{
				final Properties props = new Properties();
				props.load(stream);
				version = (String) props.get("version");
			}
			catch (Exception e)
			{
				log.warn("Could not determine version from POM properties", e);
				version = "ERROR";
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
