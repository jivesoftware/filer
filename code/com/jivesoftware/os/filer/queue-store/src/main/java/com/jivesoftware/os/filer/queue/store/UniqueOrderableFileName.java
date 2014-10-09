package com.jivesoftware.os.filer.queue.store;

import java.util.concurrent.atomic.AtomicReference;

/**
 *
 * @author jonathan.colt
 */
public class UniqueOrderableFileName {

    static final private AtomicReference<TimeAndOrder> state = new AtomicReference<>(new TimeAndOrder(System.currentTimeMillis(), 0));

    static UniqueOrderableFileName createOrderableFileName() {
        return new UniqueOrderableFileName(nextTimeAndOrder().toString());
    }

    private final String name;

    UniqueOrderableFileName(String name) {
        this.name = name;
    }

    TimeAndOrder getOrderId() {
        return new TimeAndOrder(name);
    }

    @Override
    public String toString() {
        return name;
    }

    static TimeAndOrder nextTimeAndOrder() {
        while (true) {
            long timestamp = System.currentTimeMillis();
            TimeAndOrder current = state.get();

            if (current.time > timestamp) {
                long retryWaitHint = current.time - timestamp;

                try {
                    Thread.sleep(retryWaitHint);
                } catch (InterruptedException ie) {
                    Thread.interrupted();
                }

            } else {
                TimeAndOrder next;

                if (current.time == timestamp) {
                    int nextOrder = current.order + 1;
                    next = new TimeAndOrder(timestamp, nextOrder);
                } else {
                    next = new TimeAndOrder(timestamp, 0);
                }

                if (state.compareAndSet(current, next)) {
                    return next;
                }
            }
        }
    }

    static class TimeAndOrder implements Comparable<TimeAndOrder> {

        final long time;
        final int order;

        TimeAndOrder(long time, int order) {
            this.time = time;
            this.order = order;
        }

        TimeAndOrder(String stringForm) {
            String[] timeAndOrder = stringForm.split("-");

            this.time = Long.parseLong(timeAndOrder[0]);
            this.order = Integer.parseInt(timeAndOrder[1]);
        }

        @Override
        public String toString() {
            return time + "-" + order;
        }

        @Override
        public int compareTo(TimeAndOrder o) {
            int c = Long.compare(time, o.time);
            if (c == 0) {
                c = Integer.compare(order, o.order);
            }
            return -c; // make descending
        }

    }

}
