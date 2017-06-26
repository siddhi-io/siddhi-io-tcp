package org.wso2.extension.siddhi.io.tcp.transport.utils;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by suho on 6/23/17.
 */
public class FlowController {

    private volatile boolean paused = false;
    private ReentrantLock lock;
    private Condition condition;

    public FlowController() {
        this.lock = new ReentrantLock();
        this.condition = lock.newCondition();
    }

    public void pause() {
        paused = true;
    }

    public void resume() {
        paused = false;
        try {
            lock.lock();
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public void barrier() {
        if (paused) { //spurious wakeup condition is deliberately traded off for performance
            lock.lock();
            try {
                condition.await();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } finally {
                lock.unlock();
            }
        }
    }
}
