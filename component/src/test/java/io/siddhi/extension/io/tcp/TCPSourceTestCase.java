/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.extension.io.tcp;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.source.Source;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.extension.io.tcp.transport.TCPNettyClient;
import io.siddhi.extension.io.tcp.utils.LoggerAppender;
import io.siddhi.extension.io.tcp.utils.LoggerCallBack;
import io.siddhi.extension.map.binary.sinkmapper.BinaryEventConverter;
import io.siddhi.extension.map.binary.utils.EventDefinitionConverterUtil;
import io.siddhi.query.api.definition.Attribute;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * TCP source test case.
 */

public class TCPSourceTestCase {
    static final Logger LOG = Logger.getLogger(TCPSourceTestCase.class);
    private volatile int count;
    private volatile boolean eventArrived;
    private boolean isLogEventArrived;

    @BeforeMethod
    public void init() {
        count = 0;
        eventArrived = false;
    }


    @Test
    public void testTcpSource1() throws InterruptedException, IOException, ConnectionUnavailableException {
        LOG.info("tcpSource TestCase 1");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "@app:name('foo')" +
                "@source(type='tcp', @map(type='binary'))" +
                "define stream inputStream (a string, b int, c float, d long, e double, f bool);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);


        Attribute.Type[] types = EventDefinitionConverterUtil.generateAttributeTypeArray(siddhiAppRuntime
                .getStreamDefinitionMap().get("inputStream").getAttributeList());


        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    switch (count) {
                        case 1:
                            AssertJUnit.assertEquals("test", event.getData(0));
                            break;
                        case 2:
                            AssertJUnit.assertEquals("test1", event.getData(0));
                            break;
                        case 3:
                            AssertJUnit.assertEquals("test2", event.getData(0));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });

        siddhiAppRuntime.start();

        TCPNettyClient tcpNettyClient = new TCPNettyClient();
        tcpNettyClient.connect("localhost", 9892);
        ArrayList<Event> arrayList = new ArrayList<Event>(3);

        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test", 36, 3.0f, 380L, 23.0, true}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test1", 361, 31.0f, 3801L, 231.0, false}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test2", 362, 32.0f, 3802L, 232.0, true}));
        try {
            tcpNettyClient.send("foo/inputStream", BinaryEventConverter.convertToBinaryMessage(
                    arrayList.toArray(new Event[3]), types).array()).await();
        } catch (IOException e) {
            LOG.error(e);
        }

        tcpNettyClient.disconnect();
        tcpNettyClient.shutdown();
        Thread.sleep(300);

        AssertJUnit.assertEquals(3, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();

    }


    @Test(dependsOnMethods = "testTcpSource1")
    public void testTcpSource2() throws InterruptedException, ConnectionUnavailableException {
        LOG.info("tcpSource TestCase 2");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "@app:name('foo')" +
                "@source(type='tcp', context='bar', @map(type='binary'))" +
                "define stream inputStream (a string, b int, c float, d long, e double, f bool);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        Attribute.Type[] types = EventDefinitionConverterUtil.generateAttributeTypeArray(siddhiAppRuntime
                .getStreamDefinitionMap().get("inputStream").getAttributeList());

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    switch (count) {
                        case 1:
                            AssertJUnit.assertEquals("test", event.getData(0));
                            break;
                        case 2:
                            AssertJUnit.assertEquals("test1", event.getData(0));
                            break;
                        case 3:
                            AssertJUnit.assertEquals("test2", event.getData(0));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });

        siddhiAppRuntime.start();

        TCPNettyClient tcpNettyClient = new TCPNettyClient();
        tcpNettyClient.connect("localhost", 9892);
        ArrayList<Event> arrayList = new ArrayList<Event>(3);

        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test", 36, 3.0f, 380L, 23.0, true}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test1", 361, 31.0f, 3801L, 231.0, false}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test2", 362, 32.0f, 3802L, 232.0, true}));
        try {
            tcpNettyClient.send("bar", BinaryEventConverter.convertToBinaryMessage(
                    arrayList.toArray(new Event[3]), types).array()).await();
        } catch (IOException e) {
            LOG.error(e);
        }

        tcpNettyClient.disconnect();
        tcpNettyClient.shutdown();
        Thread.sleep(300);

        AssertJUnit.assertEquals(3, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();

    }

    @Test(dependsOnMethods = "testTcpSource2")
    public void testTcpSource3() throws InterruptedException, ConnectionUnavailableException {
        LOG.info("tcpSource TestCase 3");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "@app:name('foo')" +
                "@source(type='tcp', @map(type='binary'))" +
                "define stream inputStream (a string, b int, c float, d long, e double, f bool);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        Attribute.Type[] types = EventDefinitionConverterUtil.generateAttributeTypeArray(siddhiAppRuntime
                .getStreamDefinitionMap().get("inputStream").getAttributeList());

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
            }
        });

        siddhiAppRuntime.start();

        TCPNettyClient tcpNettyClient = new TCPNettyClient();
        tcpNettyClient.connect("localhost", 9892);
        ArrayList<Event> arrayList = new ArrayList<Event>(3);

        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test", 36, 3.0f, 380L, 23.0, true}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test1", 361, 31.0f, 3801L, 231.0, false}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test2", 362, 32.0f, 3802L, 232.0, true}));
        try {
            tcpNettyClient.send("bar", BinaryEventConverter.convertToBinaryMessage(
                    arrayList.toArray(new Event[3]), types).array()).await();
        } catch (IOException e) {
            LOG.error(e);
        }

        tcpNettyClient.disconnect();
        tcpNettyClient.shutdown();
        Thread.sleep(300);

        AssertJUnit.assertFalse(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "testTcpSource3")
    public void testTcpSource4() throws InterruptedException {
        SiddhiAppRuntime siddhiAppRuntime = null;
        try {
            LOG.info("tcpSource TestCase 4");
            SiddhiManager siddhiManager = new SiddhiManager();

            String inStreamDefinition = "" +
                    "@app:name('foo')" +
                    "@source(type='tcp', context='bar', @map(type='passThrough')) " +
                    "define stream inputStream (a string, b int, c float, d long, e double, f bool); " +
                    "@source(type='tcp', context='bar', @map(type='passThrough')) " +
                    "define stream inputStream2 (a string, b int, c float, d long, e double, f bool); ";
            String query = ("@info(name = 'query1') " +
                    "from inputStream " +
                    "select *  " +
                    "insert into outputStream;");
            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            siddhiAppRuntime.start();
        } catch (SiddhiAppCreationException e) {
            AssertJUnit.assertNotNull(e);
        } finally {
            if (siddhiAppRuntime != null) {
                siddhiAppRuntime.shutdown();
            }
        }
    }

    @Test(dependsOnMethods = "testTcpSource4")
    public void testTcpSource5() throws InterruptedException, ConnectionUnavailableException {
        LOG.info("tcpSource TestCase 5");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "@app:name('foo')" +
                "@source(type='tcp', @map(type='binary'))" +
                "define stream inputStream (a string, b int, c float, d long, e double, f bool);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        Attribute.Type[] types = EventDefinitionConverterUtil.generateAttributeTypeArray(siddhiAppRuntime
                .getStreamDefinitionMap().get("inputStream").getAttributeList());

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    switch (count) {
                        case 1:
                            AssertJUnit.assertEquals("test", event.getData(0));
                            break;
                        case 2:
                            AssertJUnit.assertEquals("test1", event.getData(0));
                            break;
                        case 3:
                            AssertJUnit.assertEquals("test2", event.getData(0));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });

        siddhiAppRuntime.start();

        TCPNettyClient tcpNettyClient = new TCPNettyClient();
        tcpNettyClient.connect("localhost", 9892);
        ArrayList<Event> arrayList = new ArrayList<Event>(3);

        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test", 36, 3.0f, 380L, 23.0, true}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test1", 361, 31.0f, 3801L, 231.0, false}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test2", 362, 32.0f, 3802L, 232.0, true}));
        try {
            tcpNettyClient.send("foo/inputStream", BinaryEventConverter.convertToBinaryMessage(
                    arrayList.toArray(new Event[3]), types).array()).await();
        } catch (IOException e) {
            LOG.error(e);
        }
        tcpNettyClient.disconnect();
        tcpNettyClient.shutdown();
        Thread.sleep(300);

        AssertJUnit.assertEquals(3, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class, dependsOnMethods = "testTcpSource5")
    public void testTcpSource6() throws InterruptedException {
        SiddhiAppRuntime siddhiAppRuntime = null;
        try {
            LOG.info("tcpSource TestCase 6");
            SiddhiManager siddhiManager = new SiddhiManager();

            String inStreamDefinition = "" +
                    "@app:name('foo')" +
                    "@source(type='tcp',  @map(type='text'))" +
                    "define stream inputStream (a string, b int, c float, d long, e double, f bool);";
            String query = ("@info(name = 'query1') " +
                    "from inputStream " +
                    "select *  " +
                    "insert into outputStream;");
            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

            siddhiAppRuntime.start();
        } finally {
            if (siddhiAppRuntime != null) {
                siddhiAppRuntime.shutdown();
            }
        }
    }

    @Test(dependsOnMethods = "testTcpSource6")
    public void testTcpSource7() throws InterruptedException, ConnectionUnavailableException {
        LOG.info("tcpSource TestCase 7");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "@app:name('foo')" +
                "@source(type='tcp', context='bar', @map(type='binary'))" +
                "define stream inputStream (a string, b int, c float, d long, e double, f bool);" +
                "@source(type='tcp', context='bar1', @map(type='binary'))" +
                "define stream inputStream1 (a string, b int, c float, d long, e double, f bool);" +
                "";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;" +
                "" +
                "from inputStream1 " +
                "select *  " +
                "insert into outputStream;" +
                "");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        Attribute.Type[] types = EventDefinitionConverterUtil.generateAttributeTypeArray(siddhiAppRuntime
                .getStreamDefinitionMap().get("inputStream").getAttributeList());

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                eventArrived = true;
                for (Event event : events) {
                    count++;
                    switch (count) {
                        case 1:
                            AssertJUnit.assertEquals("test", event.getData(0));
                            break;
                        case 2:
                            AssertJUnit.assertEquals("test1", event.getData(0));
                            break;
                        case 3:
                            AssertJUnit.assertEquals("test2", event.getData(0));
                            break;
                        case 4:
                            AssertJUnit.assertEquals("test", event.getData(0));
                            break;
                        case 5:
                            AssertJUnit.assertEquals("test1", event.getData(0));
                            break;
                        case 6:
                            AssertJUnit.assertEquals("test2", event.getData(0));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }

        });

        siddhiAppRuntime.start();

        TCPNettyClient tcpNettyClient = new TCPNettyClient();
        tcpNettyClient.connect("localhost", 9892);
        ArrayList<Event> arrayList = new ArrayList<Event>(3);

        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test", 36, 3.0f, 380L, 23.0, true}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test1", 361, 31.0f, 3801L, 231.0, false}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test2", 362, 32.0f, 3802L, 232.0, true}));

        try {
            tcpNettyClient.send("bar", BinaryEventConverter.convertToBinaryMessage(
                    arrayList.toArray(new Event[3]), types).array()).await();
            tcpNettyClient.send("bar1", BinaryEventConverter.convertToBinaryMessage(
                    arrayList.toArray(new Event[3]), types).array()).await();
        } catch (IOException e) {
            LOG.error(e);
        }

        tcpNettyClient.disconnect();
        tcpNettyClient.shutdown();
        Thread.sleep(300);

        AssertJUnit.assertEquals(6, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void testTcpSource8() throws InterruptedException {
        String regexPattern = "Error starting Siddhi App 'foo'";

        SiddhiAppRuntime siddhiAppRuntime = null;
        try {
            LOG.info("tcpSource TestCase 8");
            SiddhiManager siddhiManager = new SiddhiManager();

            String inStreamDefinition = "" +
                    "@app:name('foo')" +
                    "@source(type='tcp', @map(type='binary'))" +
                    "@source(type='tcp', @map(type='binary'))" +
                    "define stream inputStream (a string, b int, c float, d long, e double, f bool);";
            String query = ("@info(name = 'query1') " +
                    "from inputStream " +
                    "select *  " +
                    "insert into outputStream;");
            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            LoggerCallBack loggerCallBack = new LoggerCallBack(regexPattern) {
                @Override
                public void receive(String logEventMessage) {
                    isLogEventArrived = true;
                }
            };
            LoggerAppender.setLoggerCallBack(loggerCallBack);
            siddhiAppRuntime.start();
            Thread.sleep(1000);
            Assert.assertEquals(isLogEventArrived, true,
                    "Matching log event not found for pattern: '" + regexPattern + "'");
            LoggerAppender.setLoggerCallBack(null);

        } finally {
            if (siddhiAppRuntime != null) {
                siddhiAppRuntime.shutdown();
            }
        }
    }

    @Test
    public void testTcpSourcePauseAndResume() throws InterruptedException, ConnectionUnavailableException {
        init();
        LOG.info("tcpSource TestCase PauseAndResume");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "@source(type='tcp', context='inputStream', @map(type='binary'))" +
                "define stream inputStream (a string, b int, c float, d long, e double, f bool);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);

        Attribute.Type[] types = EventDefinitionConverterUtil.generateAttributeTypeArray(siddhiAppRuntime
                .getStreamDefinitionMap().get("inputStream").getAttributeList());

        Collection<List<Source>> sources = siddhiAppRuntime.getSources();

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    switch (count) {
                        case 1:
                            AssertJUnit.assertEquals("test", event.getData(0));
                            break;
                        case 2:
                            AssertJUnit.assertEquals("test1", event.getData(0));
                            break;
                        case 3:
                            AssertJUnit.assertEquals("test2", event.getData(0));
                            break;
                        default:
                    }
                }
            }
        });

        siddhiAppRuntime.start();

        TCPNettyClient tcpNettyClient = new TCPNettyClient();
        tcpNettyClient.connect("localhost", 9892);

        ArrayList<Event> arrayList = new ArrayList<Event>(3);
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test", 36, 3.0f, 380L, 23.0, true}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test1", 361, 31.0f, 3801L, 231.0, false}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test2", 362, 32.0f, 3802L, 232.0, true}));
        try {
            tcpNettyClient.send("inputStream", BinaryEventConverter.convertToBinaryMessage(
                    arrayList.toArray(new Event[3]), types).array()).await();
        } catch (IOException e) {
            LOG.error(e);
        }

        TCPNettyClient tcpNettyClient2 = new TCPNettyClient();
        tcpNettyClient2.connect("localhost", 9892);

        ArrayList<Event> arrayList2 = new ArrayList<Event>(1);
        arrayList2.add(new Event(System.currentTimeMillis(), new Object[]{"test2", 36, 3.0f, 380L, 23.0, true}));
        Thread.sleep(1000);
        try {
            tcpNettyClient2.send("inputStream", BinaryEventConverter.convertToBinaryMessage(
                    arrayList2.toArray(new Event[1]), types).array()).await();
        } catch (IOException e) {
            LOG.error(e);
        }

        Thread.sleep(1000);
        AssertJUnit.assertTrue(eventArrived);
        AssertJUnit.assertEquals(4, count);
        count = 3;
        eventArrived = false;

        // pause
        sources.forEach(e -> e.forEach(Source::pause));
        Thread.sleep(1000);
        // send few events
        arrayList.clear();
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test", 36, 3.0f, 380L, 23.0, true}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test1", 361, 31.0f, 3801L, 231.0, false}));
        try {
            tcpNettyClient.send("inputStream", BinaryEventConverter.convertToBinaryMessage(
                    arrayList.toArray(new Event[2]), types).array()).await();
        } catch (IOException e) {
            LOG.error(e);
        }
        Thread.sleep(1000);
        try {
            tcpNettyClient2.send("inputStream", BinaryEventConverter.convertToBinaryMessage(
                    arrayList2.toArray(new Event[1]), types).array()).await();
        } catch (IOException e) {
            LOG.error(e);
        }

        Thread.sleep(100);
        AssertJUnit.assertFalse(eventArrived);

        // resume
        sources.forEach(e -> e.forEach(Source::resume));
        // send few more events
        arrayList.clear();
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test2", 36, 3.0f, 380L, 23.0, true}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test3", 361, 31.0f, 3801L, 231.0, false}));
        try {
            tcpNettyClient.send("inputStream", BinaryEventConverter.convertToBinaryMessage(
                    arrayList.toArray(new Event[2]), types).array()).await();
        } catch (IOException e) {
            LOG.error(e);
        }
        Thread.sleep(1000);
        // once resumed, we should be able to access the data sent while the transport is pause
        AssertJUnit.assertEquals(8, count);
        AssertJUnit.assertTrue(eventArrived);

        count = 0;

        // send few more events
        arrayList.clear();
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test", 36, 3.0f, 380L, 23.0, true}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test1", 361, 31.0f, 3801L, 231.0, false}));
        try {
            tcpNettyClient.send("inputStream", BinaryEventConverter.convertToBinaryMessage(
                    arrayList.toArray(new Event[2]), types).array()).await();
        } catch (IOException e) {
            LOG.error(e);
        }
        Thread.sleep(1000);
        AssertJUnit.assertEquals(2, count);

        tcpNettyClient.disconnect();
        tcpNettyClient2.disconnect();
        tcpNettyClient.shutdown();
        tcpNettyClient2.shutdown();
        Thread.sleep(300);
        siddhiAppRuntime.shutdown();

    }


}
