package net.greghaines.jesque;

public class InfiniteAction implements Runnable {
    public InfiniteAction() {
    }

    public void run() {
        while (true) {
            synchronized (this) {
                try {
                    this.wait();
                } catch (Exception e) {}
            }
        }
    }
}
