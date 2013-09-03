package net.greghaines.jesque.admin;

import java.util.Set;

import net.greghaines.jesque.worker.JobExecutor;
import net.greghaines.jesque.worker.Worker;

public interface Admin extends JobExecutor, Runnable {
    
    Set<String> getChannels();

    void setChannels(Set<String> channels);

    Worker getWorker();

    void setWorker(Worker worker);
}
