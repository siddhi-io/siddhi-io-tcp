package org.wso2.extension.siddhi.io.tcp.transport;

import org.wso2.siddhi.core.event.Event;

/**
 * Created by suho on 6/21/17.
 */
public class TestRun {

    public static void main(String[] args) {
        TCPNettyClient tcpNettyClient = new TCPNettyClient();
        tcpNettyClient.connect("localhost", 9892);

        new Thread(new Runner(tcpNettyClient)).start();

//        try {
//            Thread.sleep(10000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        tcpNettyClient.disconnect();
//        tcpNettyClient.shutdown();
    }

    static class Runner implements Runnable{

        private TCPNettyClient tcpNettyClient;

        public Runner(TCPNettyClient tcpNettyClient) {

            this.tcpNettyClient = tcpNettyClient;
        }

        @Override
        public void run() {
            for (int i = 0; i < 10000000; i++) {
//            ArrayList<Event> arrayList = new ArrayList<Event>(4);
//            for (int j = 0; j < 2; j++) {
//                arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"WSO2", i, 10}));
//                arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"IBM", i, 10}));
//            }
//            tcpNettyClient.send("StockStream", arrayList.toArray(new Event[4]));
//                try {
                    tcpNettyClient.send("StockStream", new Event[]{new Event(System.currentTimeMillis(),
                            new Object[]{"WSO2", i, 10})});
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//                    System.out.println("sent");
//            if(i%100==0) {
//                try {
//                    Thread.sleep(1);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
            }
        }
    }
}
