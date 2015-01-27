/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.filer.queue.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.mutable.MutableLong;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan
 */
public class FileJournalTest {

    // Test of toInstance method, of class FileJournal.
    @Test
    public void testJournal() throws Exception {
        long start = System.currentTimeMillis();
        System.out.println("testJournal");
        int add = 100000;
        String journalName = "journalATest";
        FileJournalImpl j = new FileJournalImpl(journalName);
        j.removeJournals(journalName);

        for (int i = 0; i < add; i++) {
            j.add((long) i);
        }
        j.closeAll();

        final MutableLong count = new MutableLong();
        // ensure absent key is empty
        j.getAll("noPresent", new QueueEntryStream<Long>() {
            @Override
            public Long stream(Long value) throws Exception {
                if (value == null) {
                    return value;
                }
                count.increment();
                return value;
            }
        });
        AssertJUnit.assertEquals(count.longValue(), 0);

        // ensure epected key has added items
        j.getAll(journalName, new QueueEntryStream<Long>() {
            @Override
            public Long stream(Long value) throws Exception {
                if (value == null) {
                    return value;
                }
                count.increment();
                return value;
            }
        });
        AssertJUnit.assertEquals(count.longValue(), add);
        System.out.println("##teamcity[buildStatisticValue key='journalSpeedElapse' value='" + (System.currentTimeMillis() - start) + "']");

    }

    public class FileJournalImpl extends FileJournal<Long> {

        private final String journalName;

        public FileJournalImpl(String journalName) {
            super("./testFileJournal/", 1024 * 1024 * 10, TimeUnit.MINUTES.toMillis(1));
            this.journalName = journalName;
        }

        @Override
        public Long toInstance(ByteBuffer bytes) throws IOException {
            return bytes.getLong();
        }

        @Override
        public ByteBuffer toBytes(Long v) throws IOException {
            ByteBuffer bb = ByteBuffer.allocate(8);
            bb.putLong(v);
            return bb;
        }

        @Override
        public long toTimestamp(Long v) {
            return System.currentTimeMillis();
        }

        @Override
        public String toJournalName(Long v) {
            return journalName;
        }
    }
}
