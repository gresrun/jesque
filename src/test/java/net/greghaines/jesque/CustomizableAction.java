package net.greghaines.jesque;

/**
 * @author Noam Y. Tenne
 */
public class CustomizableAction implements Runnable {

    private boolean failAction = true;

    @Override
    public void run() {
        if (failAction) {
            throw new RuntimeException("Failed to run customizable action");
        }
    }

    public void setFailAction(boolean failAction) {
        this.failAction = failAction;
    }
}
