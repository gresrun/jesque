package net.greghaines.jesque.worker;

public enum WorkerRecoveryStrategy
{
	TERMINATE,
	RECONNECT,
	PROCEED;
}
