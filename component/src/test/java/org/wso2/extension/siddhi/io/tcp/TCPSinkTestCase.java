/*
 * Copyright (c)  2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.extension.siddhi.io.tcp;

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.io.tcp.transport.TCPNettyServer;
import org.wso2.extension.siddhi.io.tcp.transport.callback.StreamListener;
import org.wso2.extension.siddhi.io.tcp.transport.config.ServerConfig;
import org.wso2.extension.siddhi.map.binary.sourcemapper.SiddhiEventConverter;
import org.wso2.extension.siddhi.map.binary.utils.EventDefinitionConverterUtil;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.nio.ByteBuffer;
import java.util.ArrayList;


/**
 * TCP sink test case.
 */
public class TCPSinkTestCase {
    static final Logger LOG = Logger.getLogger(TCPSinkTestCase.class);
    private volatile int count;
    private volatile int count1;
    private volatile boolean eventArrived;

    @BeforeMethod
    public void init() {
        count = 0;
        count1 = 0;
        eventArrived = false;
    }

    @Test
    public void testTcpSink1() throws InterruptedException {
        LOG.info("tcpSource TestCase 1");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (a string, b int, c float, d long, e double, f bool); " +
                "@sink(type='tcp', url='tcp://localhost:9892/foo', @map(type='binary')) " +
                "define stream outputStream (a string, b int, c float, d long, e double, f bool);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        final StreamDefinition streamDefinition = StreamDefinition.id("foo").attribute("a", Attribute.Type.STRING)
                .attribute("b", Attribute.Type.INT).attribute("c",
                        Attribute.Type.FLOAT).attribute("d", Attribute
                        .Type.LONG)
                .attribute("e", Attribute.Type.DOUBLE).attribute("f", Attribute.Type.BOOL);

        Attribute.Type[] types = EventDefinitionConverterUtil.generateAttributeTypeArray(
                streamDefinition.getAttributeList());
        TCPNettyServer tcpNettyServer = new TCPNettyServer();
        tcpNettyServer.addStreamListener(new StreamListener() {

            @Override
            public String getChannelId() {
                return streamDefinition.getId();
            }

            @Override
            public void onMessage(byte[] message) {
                onEvents(SiddhiEventConverter.toConvertToSiddhiEvents(ByteBuffer.wrap(message), types));
            }

            public void onEvents(Event[] events) {
                for (Event event : events) {
                    onEvent(event);
                }
            }

            public void onEvent(Event event) {
                LOG.info(event);
                eventArrived = true;
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

        });

        tcpNettyServer.start(new ServerConfig());

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        ArrayList<Event> arrayList = new ArrayList<Event>();
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test", 36, 3.0f, 380L, 23.0, true}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test1", 361, 31.0f, 3801L, 231.0, false}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test2", 362, 32.0f, 3802L, 232.0, true}));
        inputHandler.send(arrayList.toArray(new Event[3]));

        Thread.sleep(300);

        siddhiAppRuntime.shutdown();

        tcpNettyServer.shutdownGracefully();

        AssertJUnit.assertEquals(3, count);
        AssertJUnit.assertTrue(eventArrived);


    }

    @Test(expectedExceptions = SiddhiAppValidationException.class, dependsOnMethods = {"testTcpSink1"})
    public void testTcpSink2() throws InterruptedException {
        LOG.info("tcpSink TestCase 2");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (a string, b int, c float, d long, e double, f bool); " +
                "@sink(type='tcp', @map(type='binary')) " +
                "define stream outputStream (a string, b int, c float, d long, e double, f bool);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        Thread.sleep(300);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = {"testTcpSink2"})
    public void testTcpSink3() throws InterruptedException {
        LOG.info("tcpSink TestCase 3");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (a string, b int, c float, d long, e double, f bool); " +
                "@sink(type='tcp', url='tcp://127.0.0.1:9766/foo', @map(type='binary')) " +
                "define stream outputStream (a string, b int, c float, d long, e double, f bool);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        final StreamDefinition streamDefinition = StreamDefinition.id("foo").attribute("a", Attribute.Type.STRING)
                .attribute("b", Attribute.Type.INT).attribute("c",
                        Attribute.Type.FLOAT).attribute("d", Attribute.Type.LONG)
                .attribute("e", Attribute.Type.DOUBLE).attribute("f", Attribute.Type.BOOL);

        Attribute.Type[] types = EventDefinitionConverterUtil.generateAttributeTypeArray(
                streamDefinition.getAttributeList());

        TCPNettyServer tcpNettyServer = new TCPNettyServer();
        tcpNettyServer.addStreamListener(new StreamListener() {
            @Override
            public String getChannelId() {
                return streamDefinition.getId();
            }

            @Override
            public void onMessage(byte[] message) {
                onEvents(SiddhiEventConverter.toConvertToSiddhiEvents(ByteBuffer.wrap(message), types));
            }

            public void onEvents(Event[] events) {
                for (Event event : events) {
                    onEvent(event);
                }
            }

            public void onEvent(Event event) {
                LOG.info(event);
                eventArrived = true;
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

        });

        ServerConfig serverConfig = new ServerConfig();
        serverConfig.setPort(9766);
        serverConfig.setHost("127.0.0.1");
        tcpNettyServer.start(serverConfig);

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        ArrayList<Event> arrayList = new ArrayList<Event>();
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test", 36, 3.0f, 380L, 23.0, true}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test1", 361, 31.0f, 3801L, 231.0, false}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test2", 362, 32.0f, 3802L, 232.0, true}));
        inputHandler.send(arrayList.toArray(new Event[3]));

        Thread.sleep(300);

        AssertJUnit.assertEquals(3, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();

        tcpNettyServer.shutdownGracefully();

    }


    @Test(expectedExceptions = SiddhiAppCreationException.class, dependsOnMethods = {"testTcpSink3"})
    public void testTcpSink6() throws InterruptedException {
        LOG.info("tcpSink TestCase 6");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (a string, b int, c float, d long, e double, f bool); " +
                "@sink(type='tcp', url='tcp://127.0.0.1:9766/foo', @map(type='text')) " +
                "define stream outputStream (a string, b int, c float, d long, e double, f bool);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = null;
        try {
            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            siddhiAppRuntime.getInputHandler("inputStream");
            siddhiAppRuntime.start();
        } finally {
            if (siddhiAppRuntime != null) {
                siddhiAppRuntime.shutdown();
            }

        }
    }

    @Test(dependsOnMethods = {"testTcpSink6"})
    public void testTcpSink9() throws InterruptedException {
        LOG.info("tcpSink TestCase 9");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (a string, b int, c float, d long, e double, f bool); " +
                "@sink(type='tcp', url='tcp://127.0.0.1:9892/bar', @map(type='binary')) " +
                "define stream outputStream (a string, b int, c float, d long, e double, f bool);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        final StreamDefinition streamDefinition = StreamDefinition.id("foo").attribute("a", Attribute.Type.STRING)
                .attribute("b", Attribute.Type.INT).attribute("c",
                        Attribute.Type.FLOAT).attribute("d", Attribute.Type.LONG)
                .attribute("e", Attribute.Type.DOUBLE).attribute("f", Attribute.Type.BOOL);
        Attribute.Type[] types = EventDefinitionConverterUtil.generateAttributeTypeArray(
                streamDefinition.getAttributeList());

        TCPNettyServer tcpNettyServer = new TCPNettyServer();
        tcpNettyServer.addStreamListener(new StreamListener() {
            @Override
            public String getChannelId() {
                return streamDefinition.getId();
            }

            @Override
            public void onMessage(byte[] message) {
                eventArrived = true;
            }
        });

        tcpNettyServer.start(new ServerConfig());

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        ArrayList<Event> arrayList = new ArrayList<Event>();
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test", 36, 3.0f, 380L, 23.0, true}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test1", 361, 31.0f, 3801L, 231.0, false}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test2", 362, 32.0f, 3802L, 232.0, true}));
        inputHandler.send(arrayList.toArray(new Event[3]));

        Thread.sleep(300);

        AssertJUnit.assertFalse(eventArrived);
        siddhiAppRuntime.shutdown();

        tcpNettyServer.shutdownGracefully();

    }

    //todo validate LOG
    @Test(enabled = false, dependsOnMethods = {"testTcpSink9"})
    public void testTcpSink10() throws InterruptedException {
        LOG.info("tcpSink TestCase 10");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (a string, b int, c float, d long, e double, f bool); " +
                "@sink(type='tcp', url='tcp://127.0.0.1:9766/bar', @map(type='binary')) " +
                "define stream outputStream (a string, b int, c float, d long, e double, f bool);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        ArrayList<Event> arrayList = new ArrayList<Event>();
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test", 36, 3.0f, 380L, 23.0, true}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test1", 361, 31.0f, 3801L, 231.0, false}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test2", 362, 32.0f, 3802L, 232.0, true}));
        inputHandler.send(arrayList.toArray(new Event[3]));

        Thread.sleep(300);

        AssertJUnit.assertFalse(eventArrived);
        siddhiAppRuntime.shutdown();

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class, dependsOnMethods = {"testTcpSink9"})
    public void testTcpSink11() throws InterruptedException {
        LOG.info("tcpSink TestCase 11");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (a string, b int, c float, d long, e double, f bool); " +
                "@sink(type='tcp', url='tcp://127.0.0.1:{{d}}/bar', @map(type='binary')) " +
                "define stream outputStream (a string, b int, c float, d long, e double, f bool);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = null;
        try {
            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition + query);
            siddhiAppRuntime.getInputHandler("inputStream");
            siddhiAppRuntime.start();
        } finally {
            if (siddhiAppRuntime != null) {
                siddhiAppRuntime.shutdown();
            }

        }
    }

    @Test//(dependsOnMethods = {"testTcpSink11"})
    public void testTcpSink13() throws InterruptedException {
        LOG.info("tcpSink TestCase 13");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (a string, b int, c float, d long, e double, f bool); " +
                "@sink(type='tcp', url='tcp://127.0.0.1:9892/foo', @map(type='binary')) " +
                "define stream outputStream1 (a string, b int, c float, d long, e double, f bool);" +
                "@sink(type='tcp', url='tcp://127.0.0.1:9892/foo', @map(type='binary')) " +
                "define stream outputStream2 (a string, b int, c float, d long, e double, f bool);";
        String query = ("@info(name = 'query1') " +
                "" +
                "from inputStream " +
                "select *  " +
                "insert into outputStream1; " +
                "" +
                "from inputStream " +
                "select *  " +
                "insert into outputStream2; " +
                "");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        final StreamDefinition streamDefinition = StreamDefinition.id("foo").attribute("a", Attribute.Type.STRING)
                .attribute("b", Attribute.Type.INT).attribute("c",
                        Attribute.Type.FLOAT).attribute("d", Attribute.Type.LONG)
                .attribute("e", Attribute.Type.DOUBLE).attribute("f", Attribute.Type.BOOL);
        Attribute.Type[] types = EventDefinitionConverterUtil.generateAttributeTypeArray(
                streamDefinition.getAttributeList());

        TCPNettyServer tcpNettyServer = new TCPNettyServer();
        tcpNettyServer.addStreamListener(new StreamListener() {
            @Override
            public String getChannelId() {
                return streamDefinition.getId();
            }

            @Override
            public void onMessage(byte[] message) {
                onEvents(SiddhiEventConverter.toConvertToSiddhiEvents(ByteBuffer.wrap(message), types));
            }

            public void onEvents(Event[] events) {
                for (Event event : events) {
                    onEvent(event);
                }
            }

            public void onEvent(Event event) {
                LOG.info(event);
                eventArrived = true;
                count++;
                /*
                commenting this out since we cannot guarantee an event order here
                switch (count) {
                    case 1:
                        AssertJUnit.assertEquals("test", event.getMessage(0));
                        break;
                    case 2:
                        AssertJUnit.assertEquals("test1", event.getMessage(0));
                        break;
                    case 3:
                        AssertJUnit.assertEquals("test2", event.getMessage(0));
                        break;
                    case 4:
                        AssertJUnit.assertEquals("test", event.getMessage(0));
                        break;
                    case 5:
                        AssertJUnit.assertEquals("test1", event.getMessage(0));
                        break;
                    case 6:
                        AssertJUnit.assertEquals("test2", event.getMessage(0));
                        break;
                    default:
                        AssertJUnit.fail();

                }*/
            }
        });

        tcpNettyServer.start(new ServerConfig());

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        ArrayList<Event> arrayList = new ArrayList<Event>();
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test", 36, 3.0f, 380L, 23.0, true}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test1", 361, 31.0f, 3801L, 231.0, false}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test2", 362, 32.0f, 3802L, 232.0, true}));
        inputHandler.send(arrayList.toArray(new Event[3]));

        Thread.sleep(3000);

        AssertJUnit.assertEquals(6, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();

        tcpNettyServer.shutdownGracefully();

    }

    @Test(dependsOnMethods = {"testTcpSink13"})
    public void testTcpSink14() throws InterruptedException {
        LOG.info("tcpSink TestCase 14");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (a string, b int, c float, d long, e double, f bool); " +
                "@sink(type='tcp', url='tcp://127.0.0.1:9854/foo1', @map(type='binary')) " +
                "@sink(type='tcp', url='tcp://127.0.0.1:9892/foo2', @map(type='binary')) " +
                "define stream outputStream(a string, b int, c float, d long, e double, f bool);";
        String query = ("@info(name = 'query1') " +
                "" +
                "from inputStream " +
                "select *  " +
                "insert into outputStream; " +
                "");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        final StreamDefinition streamDefinition1 = StreamDefinition.id("foo1").attribute("a", Attribute.Type.STRING)
                .attribute("b", Attribute.Type.INT).attribute("c",
                        Attribute.Type.FLOAT).attribute("d", Attribute.Type.LONG)
                .attribute("e", Attribute.Type.DOUBLE).attribute("f", Attribute.Type.BOOL);
        Attribute.Type[] types1 = EventDefinitionConverterUtil.generateAttributeTypeArray(
                streamDefinition1.getAttributeList());

        final StreamDefinition streamDefinition2 = StreamDefinition.id("foo2").attribute("a", Attribute.Type.STRING)
                .attribute("b", Attribute.Type.INT).attribute("c", Attribute.Type.FLOAT).
                        attribute("d", Attribute.Type.LONG)
                .attribute("e", Attribute.Type.DOUBLE).attribute("f", Attribute.Type.BOOL);
        Attribute.Type[] types2 = EventDefinitionConverterUtil.generateAttributeTypeArray(
                streamDefinition2.getAttributeList());

        TCPNettyServer tcpNettyServer1 = new TCPNettyServer();
        TCPNettyServer tcpNettyServer2 = new TCPNettyServer();
        tcpNettyServer1.addStreamListener(new StreamListener() {
            @Override
            public String getChannelId() {
                return streamDefinition1.getId();
            }

            @Override
            public void onMessage(byte[] message) {
                onEvents(SiddhiEventConverter.toConvertToSiddhiEvents(ByteBuffer.wrap(message), types1));
            }

            public void onEvents(Event[] events) {
                for (Event event : events) {
                    onEvent(event);
                }
            }

            public void onEvent(Event event) {
                LOG.info(event);
                eventArrived = true;
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
        });

        tcpNettyServer2.addStreamListener(new StreamListener() {
            @Override
            public String getChannelId() {
                return streamDefinition2.getId();
            }

            @Override
            public void onMessage(byte[] message) {
                onEvents(SiddhiEventConverter.toConvertToSiddhiEvents(ByteBuffer.wrap(message), types2));
            }

            public void onEvents(Event[] events) {
                for (Event event : events) {
                    onEvent(event);
                }
            }

            public void onEvent(Event event) {
                LOG.info(event);
                eventArrived = true;
                count1++;
                switch (count1) {
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
        });
        ServerConfig serverConfig = new ServerConfig();
        serverConfig.setPort(9854);
        tcpNettyServer1.start(serverConfig);
        tcpNettyServer2.start(new ServerConfig());

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        ArrayList<Event> arrayList = new ArrayList<Event>();
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test", 36, 3.0f, 380L, 23.0, true}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test1", 361, 31.0f, 3801L, 231.0, false}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test2", 362, 32.0f, 3802L, 232.0, true}));
        inputHandler.send(arrayList.toArray(new Event[3]));

        Thread.sleep(3000);

        AssertJUnit.assertEquals(3, count);
        AssertJUnit.assertEquals(3, count1);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();

        tcpNettyServer1.shutdownGracefully();
        tcpNettyServer2.shutdownGracefully();

    }

    @Test(dependsOnMethods = {"testTcpSink14"})
    public void testTcpSink15() throws InterruptedException {
        LOG.info("tcpSink TestCase 15");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "@app:name('foo') " +
                "define stream inputStream (a string, b int, c float, d long, e double, f bool); " +
                "@sink(type='tcp', url='tcp://127.0.0.1:9892/foo/inputStream1', @map(type='binary')) " +
                "define stream outputStream(a string, b int, c float, d long, e double, f bool);";
        String query = ("@info(name = 'query1') " +
                "" +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;" +
                " " +
                "");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        final StreamDefinition streamDefinition1 = StreamDefinition.id("foo/inputStream1")
                .attribute("a", Attribute.Type.STRING)
                .attribute("b", Attribute.Type.INT)
                .attribute("c", Attribute.Type.FLOAT)
                .attribute("d", Attribute.Type.LONG)
                .attribute("e", Attribute.Type.DOUBLE)
                .attribute("f", Attribute.Type.BOOL);

        Attribute.Type[] types = EventDefinitionConverterUtil.generateAttributeTypeArray(
                streamDefinition1.getAttributeList());

        TCPNettyServer tcpNettyServer1 = new TCPNettyServer();
        tcpNettyServer1.addStreamListener(new StreamListener() {
            @Override
            public String getChannelId() {
                return streamDefinition1.getId();
            }

            @Override
            public void onMessage(byte[] message) {
                onEvents(SiddhiEventConverter.toConvertToSiddhiEvents(ByteBuffer.wrap(message), types));
            }

            public void onEvents(Event[] events) {
                for (Event event : events) {
                    onEvent(event);
                }
            }

            public void onEvent(Event event) {
                LOG.info(event);
                eventArrived = true;
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
        });

        tcpNettyServer1.start(new ServerConfig());

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        ArrayList<Event> arrayList = new ArrayList<Event>();
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test", 36, 3.0f, 380L, 23.0, true}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test1", 361, 31.0f, 3801L, 231.0, false}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test2", 362, 32.0f, 3802L, 232.0, true}));
        inputHandler.send(arrayList.toArray(new Event[3]));

        Thread.sleep(3000);

        AssertJUnit.assertEquals(3, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();

        tcpNettyServer1.shutdownGracefully();

    }


    @Test//(dependsOnMethods = {"testTcpSink15"})
    public void testTcpSink16() throws InterruptedException {
        LOG.info("tcpSink TestCase 16");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (a string, b int, c float, d long, e double, f bool); " +
                "@sink(type='tcp', url='tcp://127.0.0.1:9892/foo', sync='true', @map(type='binary')) " +
                "define stream outputStream (a string, b int, c float, d long, e double, f bool);";
        String query = ("@info(name = 'query1') " +
                "from inputStream " +
                "select *  " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(inStreamDefinition +
                query);

        final StreamDefinition streamDefinition = StreamDefinition.id("foo").attribute("a", Attribute.Type.STRING)
                .attribute("b", Attribute.Type.INT).attribute("c", Attribute.Type.FLOAT).
                        attribute("d", Attribute.Type.LONG)
                .attribute("e", Attribute.Type.DOUBLE).attribute("f", Attribute.Type.BOOL);
        Attribute.Type[] types = EventDefinitionConverterUtil.generateAttributeTypeArray(
                streamDefinition.getAttributeList());

        TCPNettyServer tcpNettyServer = new TCPNettyServer();
        tcpNettyServer.addStreamListener(new StreamListener() {
            @Override
            public String getChannelId() {
                return streamDefinition.getId();
            }

            @Override
            public void onMessage(byte[] message) {
                onEvents(SiddhiEventConverter.toConvertToSiddhiEvents(ByteBuffer.wrap(message), types));
            }

            public void onEvents(Event[] events) {
                for (Event event : events) {
                    onEvent(event);
                }
            }

            public void onEvent(Event event) {
                LOG.info(event);
                eventArrived = true;
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
        });


        siddhiAppRuntime.start();
        Thread.sleep(2000);

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");

        ArrayList<Event> arrayList = new ArrayList<Event>();
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test", 36, 3.0f, 380L, 23.0, true}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test1", 361, 31.0f, 3801L, 231.0, false}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"test2", 362, 32.0f, 3802L, 232.0, true}));
        inputHandler.send(arrayList.toArray(new Event[3]));

        tcpNettyServer.start(new ServerConfig());

        Thread.sleep(5000);
        inputHandler.send(arrayList.toArray(new Event[3]));

        Thread.sleep(300);
        AssertJUnit.assertEquals(3, count);
        AssertJUnit.assertTrue(eventArrived);
        tcpNettyServer.shutdownGracefully();

        eventArrived = false;
        count = 0;

        Thread.sleep(500);
        inputHandler.send(arrayList.toArray(new Event[3]));

        tcpNettyServer.start(new ServerConfig());

        AssertJUnit.assertFalse(eventArrived);
        Thread.sleep(7000);
        inputHandler.send(arrayList.toArray(new Event[3]));

        Thread.sleep(300);
        AssertJUnit.assertEquals(3, count);
        AssertJUnit.assertTrue(eventArrived);
        tcpNettyServer.shutdownGracefully();

        siddhiAppRuntime.shutdown();
    }

}

