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
package io.siddhi.extension.io.tcp.transport.utils;

import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.extension.io.tcp.transport.callback.StreamListener;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Event stream data type holder.
 */
public class StreamListenerHolder {
    private Map<String, StreamListener> streamListenerMap = new ConcurrentHashMap<String, StreamListener>();

    public StreamListener getStreamListener(String streamId) {
        return streamListenerMap.get(streamId);
    }

    public void putStreamCallback(StreamListener streamListener) {
        if (this.streamListenerMap.containsKey(streamListener.getChannelId())) {
            throw new SiddhiAppCreationException("TCP source for channelId '" + streamListener.getChannelId()
                    + "' already defined !");
        }
        this.streamListenerMap.put(streamListener.getChannelId(), streamListener);
    }

    public void removeStreamCallback(String streamId) {
        this.streamListenerMap.remove(streamId);
    }

    public int getNoOfRegisteredStreamListeners() {
        return streamListenerMap.size();
    }
}
