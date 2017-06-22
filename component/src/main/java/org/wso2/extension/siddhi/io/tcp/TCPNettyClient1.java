/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.wso2.extension.siddhi.io.tcp;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.io.tcp.transport.TCPNettyClient;
import org.wso2.extension.siddhi.io.tcp.transport.converter.BinaryEventConverter1;
import org.wso2.extension.siddhi.io.tcp.transport.handlers.EventEncoder;
import org.wso2.extension.siddhi.io.tcp.transport.handlers.EventEncoder1;
import org.wso2.extension.siddhi.io.tcp.transport.utils.EventComposite;
import org.wso2.siddhi.core.event.Event;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;

/**
 * Tcp Netty Client.
 */
public class TCPNettyClient1 {
    private static final Logger log = Logger.getLogger(TCPNettyClient1.class);

    private EventLoopGroup group;
    private Bootstrap bootstrap;
    private Channel channel;
    private String sessionId;
    private String hostAndPort;


    public TCPNettyClient1() {
        group = new NioEventLoopGroup();
        bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addFirst(
                                new EventEncoder1()
                        );
                    }
                });
    }

    public static void main(String[] args) throws IOException {
        TCPNettyClient1 tcpNettyClient = new TCPNettyClient1();
        tcpNettyClient.connect("localhost", 9892);
        for (int i = 0; i < 1000000; i++) {
            ArrayList<Event> arrayList = new ArrayList<Event>(100);
            for (int j = 0; j < 5; j++) {
                arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"WSO2", i, 10}));
                arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"IBM", i, 10}));
            }
            try {
                tcpNettyClient.send("StockStream",
                        BinaryEventConverter1.convertToBinaryMessage(arrayList.toArray(new Event[10])).array()).sync();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        tcpNettyClient.disconnect();
        tcpNettyClient.shutdown();
    }

    public void connect(String host, int port) {
        // Start the connection attempt.
        try {
            hostAndPort = host + ":" + port;
            channel = bootstrap.connect(host, port).sync().channel();
            sessionId = UUID.randomUUID() + "-" + hostAndPort;
        } catch (InterruptedException e) {
            log.error("Error connecting to '" + hostAndPort + "', " + e.getMessage(), e);
        }
    }

    public ChannelFuture send(final String streamId, final byte[] data) {
        EventComposite eventComposite = new EventComposite(sessionId, streamId, null, data);
        ChannelFuture cf = channel.writeAndFlush(eventComposite);
        cf.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (!future.isSuccess()) {
                    log.error("Error sending events to '" + hostAndPort + "' on stream '" + streamId +
                            "', " + future.cause() + ", dropping events " , future.cause
                            ());
                }
            }
        });
        return cf;
    }

    public void disconnect() {
        if (channel != null && channel.isOpen()) {
            try {
                channel.close();
                channel.closeFuture().sync();
            } catch (InterruptedException e) {
                log.error("Error closing connection to '" + hostAndPort + "' from client '" + sessionId +
                        "', " + e);
            }
            channel.disconnect();
            log.info("Disconnecting client to '" + hostAndPort + "' with sessionId:" + sessionId);
        }
    }

    public void shutdown() {
        disconnect();
        if (group != null) {
            group.shutdownGracefully();
        }
        log.info("Stopping client to '" + hostAndPort + "' with sessionId:" + sessionId);
        hostAndPort = null;
        sessionId = null;
    }

}



