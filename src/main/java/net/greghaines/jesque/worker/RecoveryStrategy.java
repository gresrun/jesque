package net.greghaines.jesque.worker;

public enum RecoveryStrategy {
    
    TERMINATE, 
    RECONNECT, 
    PROCEED;
}
