/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.filer.queue.guaranteed.delivery;

import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.queue.processor.PhasedQueueProcessorConfig;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan
 */
public class FileQueueBackGuarenteedDeliveryFactoryTest {

    public FileQueueBackGuarenteedDeliveryFactoryTest() {
    }

    /**
     * Test of createService method, of class FileQueueBackGuaranteedDeliveryFactory.
     */
    @Test
    public void testCreateService() throws Exception {
        final List<Long> delivered = new ArrayList<Long>();
        DeliveryCallback deliveryCallback = new DeliveryCallback() {
            @Override
            public boolean deliver(Iterable<byte[]> deliver) {
                for (byte[] d : deliver) {
                    System.out.println("Delivered value=" + FilerIO.bytesLong(d));
                    delivered.add(FilerIO.bytesLong(d));
                }
                return true;
            }
        };

        PhasedQueueProcessorConfig processorConfig = PhasedQueueProcessorConfig.newBuilder("testProcessor").build();
        FileQueueBackGuaranteedDeliveryServiceConfig serviceConfig = FileQueueBackGuaranteedDeliveryServiceConfig.newBuilder("./",
                "test", processorConfig).setDeleteOnExit(true).build();
        GuaranteedDeliveryService service = FileQueueBackGuaranteedDeliveryFactory.createService(serviceConfig, deliveryCallback);


        List<byte[]> add = new ArrayList<>();
        add.add(FilerIO.longBytes(0));
        add.add(FilerIO.longBytes(1));
        add.add(FilerIO.longBytes(2));
        service.add(add);

        while (service.getStatus().undelivered() > 0) {
            Thread.sleep(1000);
        }

        Assert.assertTrue(delivered.get(0) == 0L);
        Assert.assertTrue(delivered.get(1) == 1L);
        Assert.assertTrue(delivered.get(2) == 2L);

//        service.close();

    }
}
