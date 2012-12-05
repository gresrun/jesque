package net.greghaines.jesque.worker;

public interface ExceptionHandler
{
	RecoveryStrategy onException(JobExecutor jobExecutor, Exception exception, String curQueue);
}
