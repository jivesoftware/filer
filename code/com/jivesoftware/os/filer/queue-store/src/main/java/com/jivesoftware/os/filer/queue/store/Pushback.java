/*
 * Copyright 2015 Jive Software.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.filer.queue.store;

/**
 *
 * @author jonathan.colt
 */
public class Pushback {

    public static long pushedback = 0;
    private static final Runtime runtime = Runtime.getRuntime();

    public static void pushback(String name, double memoryLoadMax) {
        long total = runtime.totalMemory();
        long free = runtime.freeMemory();
        double memoryLoad = (double) (total - free) / (double) total;
        if (memoryLoad > memoryLoadMax) {
            pushedback++;
            // could retard progress by sleeping thread
            // for now we will throw an exception
            throw new RuntimeException("Service isn't keeping UP!");
        }
    }

    public static void queueDepthPushable(String name, long size, long pushbackAtQueueSize) {
        if (pushbackAtQueueSize > 0 && size > pushbackAtQueueSize) {
            pushedback++;
            throw new RuntimeException("Unable to append because queue=" + name + " is at its pushback capacity of " + pushbackAtQueueSize);
        }
    }
}
