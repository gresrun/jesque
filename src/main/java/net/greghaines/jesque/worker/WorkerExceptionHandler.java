package net.greghaines.jesque.worker;

public interface WorkerExceptionHandler
{
	WorkerRecoveryStrategy onException(Worker worker, Exception exception, String curQueue);
}
