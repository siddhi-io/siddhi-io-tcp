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

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.SystemParameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.stream.ServiceDeploymentInfo;
import io.siddhi.core.stream.input.source.Source;
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.OptionHolder;
import org.wso2.extension.siddhi.io.tcp.transport.callback.StreamListener;
import org.wso2.extension.siddhi.io.tcp.transport.config.ServerConfig;
import org.wso2.extension.siddhi.io.tcp.transport.utils.Constant;

import java.util.Arrays;

/**
 * Tcp source extension.
 */
@Extension(
        name = "tcp",
        namespace = "source",
        description = "A Siddhi application can be configured to receive events via the TCP transport by adding " +
                "the @Source(type = 'tcp') annotation at the top of an event stream definition.\n" +
                "\n" +
                "When this is defined the associated stream will receive events from the TCP transport on " +
                "the host and port defined in the system.",
        parameters = {
                @Parameter(
                        name = "context",
                        description = "The URL 'context' that should be used to receive the events.",
                        defaultValue = "<execution plan name>/<stream name>",
                        optional = true,
                        type = DataType.STRING
                )
        },
        systemParameter = {
                @SystemParameter(
                        name = "host",
                        description = "Tcp server host.",
                        defaultValue = "0.0.0.0",
                        possibleParameters = "Any valid host or IP"
                ),
                @SystemParameter(
                        name = "port",
                        description = "Tcp server port.",
                        defaultValue = "9892",
                        possibleParameters = "Any integer representing valid port"
                ),
                @SystemParameter(
                        name = "receiver.threads",
                        description = "Number of threads to receive connections.",
                        defaultValue = "10",
                        possibleParameters = "Any positive integer"
                ),
                @SystemParameter(
                        name = "worker.threads",
                        description = "Number of threads to serve events.",
                        defaultValue = "10",
                        possibleParameters = "Any positive integer"
                ),
                @SystemParameter(
                        name = "tcp.no.delay",
                        description = "This is to specify whether to disable Nagle algorithm during message passing." +
                                "\n" +
                                "If tcp.no.delay = 'true', the execution of Nagle algorithm  will be disabled in the " +
                                "underlying TCP logic. Hence there will be no delay between two successive writes to " +
                                "the TCP connection.\n" +
                                "Else there can be a constant ack delay.",
                        defaultValue = "true",
                        possibleParameters = {"true", "false"}
                ),
                @SystemParameter(
                        name = "keep.alive",
                        description = "This property defines whether the server should be kept alive when " +
                                "there are no connections available.",
                        defaultValue = "true",
                        possibleParameters = {"true", "false"}
                )
        },
        examples = {
                @Example(
                        syntax = "" +
                                "@Source(type = 'tcp', context='abc', @map(type='binary'))\n" +
                                "define stream Foo (attribute1 string, attribute2 int );",
                        description = "" +
                                "Under this configuration, events are received via the TCP transport on default host," +
                                "port, `abc` context, and they are passed to `Foo` stream for processing. "

                )
        }
)
public class TCPSource extends Source {

    private static final String RECEIVER_THREADS = "receiver.threads";
    private static final String WORKER_THREADS = "worker.threads";
    private static final String PORT = "port";
    private static final String HOST = "host";
    private static final String TCP_NO_DELAY = "tcp.no.delay";
    private static final String KEEP_ALIVE = "keep.alive";
    private static final String CONTEXT = "context";
    private SourceEventListener sourceEventListener;
    private String context;
    private ServerConfig serverConfig;
    private ServiceDeploymentInfo serviceDeploymentInfo;


    @Override
    public StateFactory init(SourceEventListener sourceEventListener, OptionHolder optionHolder,
                             String[] requestedTransportProperties, ConfigReader configReader,
                             SiddhiAppContext siddhiAppContext) {
        if (requestedTransportProperties != null && requestedTransportProperties.length > 0) {
            throw new SiddhiAppCreationException("'tcp' source does not support requestedTransportProperties," +
                    " but at stream '" + getStreamDefinition().getId() + "' '" +
                    Arrays.deepToString(requestedTransportProperties) +
                    "' transport properties are requested");
        }
        this.sourceEventListener = sourceEventListener;
        context = optionHolder.validateAndGetStaticValue(CONTEXT,
                siddhiAppContext.getName() + "/" + sourceEventListener.getStreamDefinition().getId());

        serverConfig = new ServerConfig();
        serverConfig.setHost(configReader.readConfig(HOST, Constant.DEFAULT_HOST));
        serverConfig.setPort(Integer.parseInt(configReader.readConfig(PORT, "" + Constant.DEFAULT_PORT)));
        serverConfig.setKeepAlive(Boolean.parseBoolean((configReader.readConfig(KEEP_ALIVE, "" + Constant
                .DEFAULT_KEEP_ALIVE))));
        serverConfig.setTcpNoDelay(Boolean.parseBoolean((configReader.readConfig(TCP_NO_DELAY, "" + Constant
                .DEFAULT_TCP_NO_DELAY))));
        serverConfig.setReceiverThreads(Integer.parseInt((configReader.readConfig(RECEIVER_THREADS, "" + Constant
                .DEFAULT_RECEIVER_THREADS))));
        serverConfig.setWorkerThreads(Integer.parseInt((configReader.readConfig(WORKER_THREADS, "" + Constant
                .DEFAULT_WORKER_THREADS))));
        serviceDeploymentInfo = new ServiceDeploymentInfo(serverConfig.getPort(), false);
        return null;
    }

    @Override
    public void connect(ConnectionCallback connectionCallback, State state) throws ConnectionUnavailableException {
        TCPServer.getInstance().addStreamListener(new StreamListener() {
            @Override
            public String getChannelId() {
                return context;
            }

            @Override
            public void onMessage(byte[] message) {
                sourceEventListener.onEvent(message, null);
            }
        });
        TCPServer.getInstance().start(serverConfig);
    }

    @Override
    protected ServiceDeploymentInfo exposeServiceDeploymentInfo() {
        return serviceDeploymentInfo;
    }

    @Override
    public Class[] getOutputEventClasses() {
        return new Class[]{byte[].class};
    }

    @Override
    public void disconnect() {
        TCPServer.getInstance().removeStreamListener(context);
        TCPServer.getInstance().stop();
    }

    @Override
    public void destroy() {
    }

    @Override
    public void pause() {
        TCPServer.getInstance().pause();
    }

    @Override
    public void resume() {
        TCPServer.getInstance().resume();
    }
}
