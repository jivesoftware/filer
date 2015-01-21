/*
 * Copyright 2014 Jive Software.
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
package com.jivesoftware.os.filer.chunk.store;

import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

/**
 *
 * @author jonathan.colt
 */
public class ChunkMetrics {

    static public interface ChunkMetricMXBean {

        public long getValue();

        public String getType();
    }

    static class ChunkMetric implements ChunkMetricMXBean {

        private volatile long count;

        public void inc(int amount) {
            count += amount;
        }

        @Override
        public long getValue() {
            return count;
        }

        @Override
        public String getType() {
            return "ChunkMetric";
        }

    }

    static Map<String, ChunkMetric> metrics = new ConcurrentHashMap<>();

    static ChunkMetric get(String... name) {
        StringBuilder sb = new StringBuilder();
        for (String n : name) {
            sb.append(n);
            sb.append('.');
        }
        ChunkMetric metric = metrics.get(sb.toString());
        if (metric == null) {
            metric = new ChunkMetric();
            metrics.put(sb.toString(), metric);
            register(name, metric);
        }
        return metric;

    }

    static private void register(String[] path, Object mbean) {

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < path.length; i++) {
            if (i != 0) {
                sb.append(",");
            }
            sb.append("leaf");
            sb.append(i);
            sb.append("=");
            sb.append(path[i]);
        }

        Class clazz = mbean.getClass();
        String objectName = "ChunkStore:type=" + clazz.getSimpleName() + "," + sb.toString();

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

        try {
            ObjectName mbeanName = new ObjectName(objectName);

            // note: unregister any previous, as this may be a replacement
            if (mbs.isRegistered(mbeanName)) {
                mbs.unregisterMBean(mbeanName);
            }

            mbs.registerMBean(mbean, mbeanName);

        } catch (MalformedObjectNameException | NotCompliantMBeanException |
            InstanceAlreadyExistsException | InstanceNotFoundException | MBeanRegistrationException e) {
            System.out.println("unable to register bean: " + objectName + "cause: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private ChunkMetrics() {
    }
}
