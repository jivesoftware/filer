package com.jivesoftware.os.filer.io.chunk;

/**
 *
 * @author jonathan.colt
 */
public class AutoGrowingRecycler<R> {

    private int i;
    private R[] instances;

    public AutoGrowingRecycler(R[] instances) {
        this.instances = instances;
    }

    public R recycle() {
        if (i > 0 && instances[i - 1] != null) {
            i--;
            R instance = instances[i];
            instances[i] = null;
            return instance;
        } else {
            return null;
        }
    }

    public void recycle(R instance) {
        if (i < instances.length) {
            instances[i] = instance;
            i++;
        }

        if (i >= instances.length) {
            R[] newInstances = (R[])new Object[instances.length * 2];
            System.arraycopy(instances, 0, newInstances, 0, instances.length);
            instances = newInstances;
        }
        if (i < instances.length) {
            instances[i] = instance;
            i++;
        }
    }
}
