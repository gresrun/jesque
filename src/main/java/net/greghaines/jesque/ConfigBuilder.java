package net.greghaines.jesque;

public class ConfigBuilder
{
	/** localhost */
	public static final String DEFAULT_HOST = "localhost";
	/** 6379 */
	public static final int DEFAULT_PORT = 6379;
	/** 5 seconds */
	public static final int DEFAULT_TIMEOUT = 5000;
	/** All resque clients use "resque" by default */
	public static final String DEFAULT_NAMESPACE = "resque";
	
	public static Config getDefaultConfig()
	{
		return new ConfigBuilder().build();
	}
	
	private String host = DEFAULT_HOST;
	private int port = DEFAULT_PORT;
	private int timeout = DEFAULT_TIMEOUT; 
	private String namespace = DEFAULT_NAMESPACE;
	private Integer database = null;
	private String jobPackage = null;
	
	public ConfigBuilder(){}
	
	public ConfigBuilder(final Config startingPoint)
	{
		if (startingPoint == null)
		{
			throw new IllegalArgumentException("startingPoint must not be null");
		}
		this.host = startingPoint.getHost();
		this.port = startingPoint.getPort();
		this.timeout = startingPoint.getTimeout();
		this.namespace = startingPoint.getNamespace();
		this.database = startingPoint.getDatabase();
		this.jobPackage = startingPoint.getJobPackage();
	}
	
	public ConfigBuilder withHost(final String host)
	{
		this.host = host;
		return this;
	}
	
	public ConfigBuilder withPort(final int port)
	{
		this.port = port;
		return this;
	}
	
	public ConfigBuilder withTimeout(final int timeout)
	{
		this.timeout = timeout;
		return this;
	}
	
	public ConfigBuilder withNamespace(final String namespace)
	{
		this.namespace = namespace;
		return this;
	}
	
	public ConfigBuilder withDatabase(final Integer database)
	{
		this.database = database;
		return this;
	}
	
	public ConfigBuilder withJobPackage(final String jobPackage)
	{
		this.jobPackage = jobPackage;
		return this;
	}
	
	public Config build()
	{
		return new Config(this.host, this.port, this.timeout, this.namespace, this.database, this.jobPackage);
	}
}
