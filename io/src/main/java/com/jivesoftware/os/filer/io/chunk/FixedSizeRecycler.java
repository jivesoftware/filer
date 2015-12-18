package com.jivesoftware.os.filer.io.chunk;

/**
 *
 * @author jonathan.colt
 */
public class FixedSizeRecycler<R> {

    private int i;
    private R[] instances;

    public FixedSizeRecycler(R[] instances) {
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
    }
}
