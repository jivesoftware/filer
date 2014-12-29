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
package com.jivesoftware.os.filer.chunk.store.transaction;

import com.jivesoftware.os.filer.io.ContextStack;
import java.util.Random;

/**
 *
 * @author jonathan.colt
 */
public class StackVsNew {

    public static void main(String[] args) {
        Random random = new Random(12345);
        int outer = 100;
        int it = 10_000_000;
        random = new Random(12345);
        long best = Long.MAX_VALUE;
        long worst = 0;
        for (int o = 0; o < outer; o++) {
            long start = System.currentTimeMillis();
            int count = 1;
            for (int i = 0; i < it; i++) {
                count += random.nextInt(10);
                if (count % 100000000 == 0) {
                    System.out.println(count);
                }
            }
            long elapse = (System.currentTimeMillis() - start);
            if (elapse < best) {
                best = elapse;
            }
            if (elapse > worst) {
                worst = elapse;
            }
        }
        System.out.println("Baseline:" + best + " " + worst);

        random = new Random(12345);
        best = Long.MAX_VALUE;
        worst = 0;
        ContextStack stack = new ContextStack(1);
        for (int o = 0; o < outer; o++) {
            StackCounter stackCounter = new StackCounter();
            long start = System.currentTimeMillis();
            for (int i = 0; i < it; i++) {
                long amount = random.nextInt(10);
                stack.push(amount);
                stackCounter.inc(stack);
                long current = stack.pop();
                if (current % 100000000 == 0) {
                    System.out.println(current);
                }
            }
            long elapse = (System.currentTimeMillis() - start);
            if (elapse < best) {
                best = elapse;
            }
            if (elapse > worst) {
                worst = elapse;
            }
        }
        System.out.println("Stack:" + best + " " + worst);

        random = new Random(12345);
        best = Long.MAX_VALUE;
        worst = 0;
        for (int o = 0; o < outer; o++) {
            Counter counter = new Counter();
            long start = System.currentTimeMillis();
            for (int i = 0; i < it; i++) {
                final long amount = random.nextInt(10);
                Long current = counter.inc(new Amount() {

                    @Override
                    public Long inc(Long count) {
                        return count + amount;
                    }
                });
                if (current % 100000000 == 0) {
                    System.out.println(current);
                }
            }
            long elapse = (System.currentTimeMillis() - start);
            if (elapse < best) {
                best = elapse;
            }
            if (elapse > worst) {
                worst = elapse;
            }
        }
        System.out.println("Instance:" + best + " " + worst);

        random = new Random(12345);
        best = Long.MAX_VALUE;
        worst = 0;
        for (int o = 0; o < outer; o++) {
            Counter counter = new Counter();
            long start = System.currentTimeMillis();
            for (int i = 0; i < it; i++) {
                final long amount = random.nextInt(10);
                long current = counter.inc(new ConcreteAmount(amount));
                if (current % 100000000 == 0) {
                    System.out.println(current);
                }
            }
            long elapse = (System.currentTimeMillis() - start);
            if (elapse < best) {
                best = elapse;
            }
            if (elapse > worst) {
                worst = elapse;
            }
        }
        System.out.println("Concrete:" + best + " " + worst);

    }

    static class ConcreteAmount implements Amount {

        Long amount;

        public ConcreteAmount(Long amount) {
            this.amount = amount;
        }

        @Override
        public Long inc(Long count) {
            return count + amount;
        }

    }

    static class StackCounter {

        long count = 1;

        void inc(ContextStack contextStack) {
            long amount = contextStack.pop();
            count += amount;
            contextStack.push(count);
        }
    }

    static class Counter {

        long count = 1;

        long inc(Amount amount) {
            count = amount.inc(count);
            return count;
        }
    }

    static interface Amount {

        Long inc(Long count);
    }
}
