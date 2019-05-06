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

package io.siddhi.extension.io.tcp.transport.callback;

import org.apache.log4j.Logger;

import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Stream Listener to log the messages.
 */
public class LogStreamListener implements StreamListener {
    private static final Logger log = Logger.getLogger(LogStreamListener.class);
    private AtomicLong count = new AtomicLong(0);
    private String channelId;

    public LogStreamListener(String channelId) {
        this.channelId = channelId;
    }

    @Override
    public String getChannelId() {
        return channelId;
    }

    @Override
    public void onMessage(byte[] message) {
        log.info(count.incrementAndGet() + " " + new String(message, Charset.defaultCharset()));
    }
}
