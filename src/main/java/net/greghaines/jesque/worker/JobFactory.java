package net.greghaines.jesque.worker;

import net.greghaines.jesque.Job;

public interface JobFactory {

	public Object materializeJob(Job job) throws ClassNotFoundException;

}
