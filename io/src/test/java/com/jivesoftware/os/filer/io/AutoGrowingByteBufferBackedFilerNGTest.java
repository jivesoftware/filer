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
package com.jivesoftware.os.filer.io;

import com.jivesoftware.os.filer.io.api.StackBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class AutoGrowingByteBufferBackedFilerNGTest {

    @Test
    public void writeALongTest() throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        for (int i = 1; i < 10; i++) {
            Path createTempDirectory = Files.createTempDirectory("writeALongTest");

            ByteBufferFactory[] bufferFactorys = new ByteBufferFactory[]{
                new HeapByteBufferFactory(),
                new DirectByteBufferFactory(),
                new FileBackedMemMappedByteBufferFactory("f", 0, createTempDirectory.toFile())
            };
            for (ByteBufferFactory bf : bufferFactorys) {

                System.out.println("i:" + i);
                AutoGrowingByteBufferBackedFiler filer = new AutoGrowingByteBufferBackedFiler(bf, i, i);
                FilerIO.writeLong(filer, Long.MAX_VALUE, "a long", stackBuffer);
                filer.seek(0);
                Assert.assertEquals(FilerIO.readLong(filer, "a long", stackBuffer), Long.MAX_VALUE, "Booya");
            }
        }
    }

    @Test
    public void writeByteTest() throws Exception {
        AutoGrowingByteBufferBackedFilerDuplicateBuffer duplicateBuffer = new AutoGrowingByteBufferBackedFilerDuplicateBuffer(8);
        for (int b = 1; b < 10; b++) {
            System.out.println("b:" + b);
            Path createTempDirectory = Files.createTempDirectory("writeIntsTest");

            ByteBufferFactory[] bufferFactorys = new ByteBufferFactory[]{
                new HeapByteBufferFactory(),
                new DirectByteBufferFactory(),
                new FileBackedMemMappedByteBufferFactory("f", 0, createTempDirectory.toFile())
            };
            for (ByteBufferFactory bf : bufferFactorys) {

                AutoGrowingByteBufferBackedFiler filer = new AutoGrowingByteBufferBackedFiler(bf, b, b);
                for (int i = 0; i < b * 4; i++) {
                    System.out.println(b + " " + i + " " + bf);
                    filer.write(i);
                    filer.seek(i);
                    Assert.assertEquals(filer.read(), i, "Boo " + i + " at " + b + " " + bf);
                }
            }
        }
    }

    @Test
    public void writeIntsTest() throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        for (int b = 1; b < 10; b++) {
            System.out.println("b:" + b);
            Path createTempDirectory = Files.createTempDirectory("writeIntsTest");

            ByteBufferFactory[] bufferFactorys = new ByteBufferFactory[]{
                new HeapByteBufferFactory(),
                new DirectByteBufferFactory(),
                new FileBackedMemMappedByteBufferFactory("f", 0, createTempDirectory.toFile())
            };
            for (ByteBufferFactory bf : bufferFactorys) {

                AutoGrowingByteBufferBackedFiler filer = new AutoGrowingByteBufferBackedFiler(bf, b, b);
                for (int i = 0; i < b * 4; i++) {
                    System.out.println(b + " " + i + " " + bf);
                    FilerIO.writeInt(filer, i, "", stackBuffer);
                    filer.seek(i * 4);
                    Assert.assertEquals(FilerIO.readInt(filer, "", stackBuffer), i, "Boo " + i + " at " + b + " " + bf);
                }
            }
        }
    }

    @Test
    public void writeIntsFileBackedTest() throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        for (int b = 4; b < 10; b++) {
            System.out.println("b:" + b);
            Path createTempDirectory = Files.createTempDirectory("writeIntsTest");

            FileBackedMemMappedByteBufferFactory bf = new FileBackedMemMappedByteBufferFactory("f", 0, createTempDirectory.toFile());

            AutoGrowingByteBufferBackedFiler filer = new AutoGrowingByteBufferBackedFiler(bf, 1, b);
            for (int i = 0; i < b * 4; i++) {
                System.out.println(b + " " + i + " " + bf);
                FilerIO.writeInt(filer, i, "", stackBuffer);
                filer.seek(i * 4);
                Assert.assertEquals(FilerIO.readInt(filer, "", stackBuffer), i, "Boo " + i + " at " + b + " " + bf);
            }

        }
    }

}
