package net.greghaines.jesque.worker;

public interface WorkerPoolMXBean {
    /**
     * @return number of total workers
     */
    int getTotalWorkers();

    /**
     * @return number of active (processing) workers
     */
    int getActiveWorkers();

    /**
     * @return number of idle workers
     */
    int getIdleWorkers();
}
