package net.greghaines.jesque.admin.commands;

import java.io.Serializable;

import net.greghaines.jesque.worker.Worker;
import net.greghaines.jesque.worker.WorkerAware;

public class ShutdownCommand implements Runnable, WorkerAware, Serializable
{
	private static final long serialVersionUID = -4555815122109572121L;
	
	private final boolean now;
	private transient Worker worker;
	
	public ShutdownCommand(final boolean now)
	{
		this.now = now;
	}
	
	public void run()
	{
		if (this.worker == null)
		{
			throw new IllegalStateException("worker was not injected");
		}
		this.worker.end(this.now);
	}

	public void setWorker(final Worker worker)
	{
		this.worker = worker;
	}
}
