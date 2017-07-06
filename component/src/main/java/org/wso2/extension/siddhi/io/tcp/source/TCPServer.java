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

package org.wso2.extension.siddhi.io.tcp.source;


import org.wso2.extension.siddhi.io.tcp.transport.TCPNettyServer;
import org.wso2.extension.siddhi.io.tcp.transport.callback.StreamListener;
import org.wso2.extension.siddhi.io.tcp.transport.config.ServerConfig;

/**
 * Singleton tcp server.
 */
public class TCPServer {
    private static TCPServer ourInstance = new TCPServer();
    private TCPNettyServer tcpNettyServer = new TCPNettyServer();
    private boolean started = false;

    private TCPServer() {
    }

    public static TCPServer getInstance() {
        return ourInstance;
    }

    public synchronized void start(ServerConfig serverConfig) {
        if (!started) {
            tcpNettyServer.start(serverConfig);
            started = true;
        }
    }

    public synchronized void stop() {
        if (started) {
            if (tcpNettyServer.getNoOfRegisteredStreamListeners() == 0) {
                try {
                    tcpNettyServer.shutdownGracefully();
                } finally {
                    started = false;
                }
            }
        }
    }


    public synchronized void addStreamListener(StreamListener streamListener) {
        tcpNettyServer.addStreamListener(streamListener);
    }

    public synchronized void removeStreamListener(String streamId) {
        tcpNettyServer.removeStreamListener(streamId);
    }

    public void pause() {
        tcpNettyServer.pause();
    }

    public void resume() {
        tcpNettyServer.resume();
    }
}
