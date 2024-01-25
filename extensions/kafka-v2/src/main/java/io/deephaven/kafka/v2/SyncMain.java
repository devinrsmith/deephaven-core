package io.deephaven.kafka.v2;

import java.util.concurrent.CountDownLatch;

public class SyncMain {

    private final Object myLock = new Object();
    private int count = 0;

    public synchronized void doBlockingThingOnAnObject(int timeoutMillis) throws InterruptedException {
        ++count;
        synchronized (myLock) {
            // does this wait cause this to become available?
            myLock.wait(timeoutMillis);
        }
        ++count;
    }

    public synchronized int getCount() {
        return count;
    }

    public static void main(String[] args) throws InterruptedException {

        final SyncMain x = new SyncMain();

        final Thread thread = new Thread(() -> {
            while (true) {
                try {
                    x.doBlockingThingOnAnObject(1000);
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        });
        thread.setDaemon(true);
        thread.start();

        for (int i = 0; i < 100; ++i) {
            System.out.println(x.getCount());
            Thread.sleep(1);
        }
    }
}
