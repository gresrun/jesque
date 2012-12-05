package net.greghaines.jesque.admin.commands;

import java.io.Serializable;

import net.greghaines.jesque.worker.Worker;
import net.greghaines.jesque.worker.WorkerAware;

public class PauseCommand implements Runnable, WorkerAware, Serializable
{
	private static final long serialVersionUID = -4555815122109572121L;
	
	private final boolean pause;
	private transient Worker worker;
	
	public PauseCommand(final boolean pause)
	{
		this.pause = pause;
	}
	
	public void run()
	{
		if (this.worker == null)
		{
			throw new IllegalStateException("worker was not injected");
		}
		this.worker.togglePause(this.pause);
	}

	public void setWorker(final Worker worker)
	{
		this.worker = worker;
	}
}
