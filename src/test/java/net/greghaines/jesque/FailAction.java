package net.greghaines.jesque;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FailAction implements Runnable
{
	private static final Logger log = LoggerFactory.getLogger(FailAction.class);

	public FailAction(){}

	public void run()
	{
		log.info("FailAction.run()");
		try
		{
			throw new IOException("poof.");
		}
		catch (IOException ioe)
		{
			throw new RuntimeException("BOOM!", ioe);
		}
	}
}
