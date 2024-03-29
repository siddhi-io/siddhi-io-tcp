# API Docs - v3.0.6

!!! Info "Tested Siddhi Core version: *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/">5.1.21</a>*"
    It could also support other Siddhi Core minor versions.

## Sink

### tcp *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#sink">(Sink)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">A Siddhi application can be configured to publish events via the TCP transport by adding the @Sink(type = 'tcp') annotation at the top of an event stream definition.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
@sink(type="tcp", url="<STRING>", sync="<STRING>", tcp.no.delay="<BOOL>", keep.alive="<BOOL>", worker.threads="<INT|LONG>", @map(...)))
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">url</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The URL to which outgoing events should be published via TCP.<br>The URL should adhere to <code>tcp://&lt;host&gt;:&lt;port&gt;/&lt;context&gt;</code> format.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">sync</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This parameter defines whether the events should be published in a synchronized manner or not.<br>If sync = 'true', then the worker will wait for the ack after sending the message.<br>Else it will not wait for an ack.</p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">tcp.no.delay</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This is to specify whether to disable Nagle algorithm during message passing.<br>If tcp.no.delay = 'true', the execution of Nagle algorithm will be disabled in the underlying TCP logic. Hence there will be no delay between two successive writes to the TCP connection.<br>Else there can be a constant ack delay.</p></td>
        <td style="vertical-align: top">true</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">keep.alive</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This property defines whether the server should be kept alive when there are no connections available.</p></td>
        <td style="vertical-align: top">true</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">worker.threads</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Number of threads to publish events.</p></td>
        <td style="vertical-align: top">10</td>
        <td style="vertical-align: top">INT<br>LONG</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@Sink(type = 'tcp', url='tcp://localhost:8080/abc', sync='true' 
   @map(type='binary'))
define stream Foo (attribute1 string, attribute2 int);
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">A sink of type 'tcp' has been defined.<br>All events arriving at Foo stream via TCP transport will be sent to the url tcp://localhost:8080/abc in a synchronous manner.</p>
<p></p>
## Source

### tcp *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#source">(Source)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">A Siddhi application can be configured to receive events via the TCP transport by adding the @Source(type = 'tcp') annotation at the top of an event stream definition.<br><br>When this is defined the associated stream will receive events from the TCP transport on the host and port defined in the system.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
@source(type="tcp", context="<STRING>", @map(...)))
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">context</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The URL 'context' that should be used to receive the events.</p></td>
        <td style="vertical-align: top"><execution plan name>/<stream name></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="system-parameters" class="md-typeset" style="display: block; font-weight: bold;">System Parameters</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Parameters</th>
    </tr>
    <tr>
        <td style="vertical-align: top">host</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">Tcp server host.</p></td>
        <td style="vertical-align: top">0.0.0.0</td>
        <td style="vertical-align: top">Any valid host or IP</td>
    </tr>
    <tr>
        <td style="vertical-align: top">port</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">Tcp server port.</p></td>
        <td style="vertical-align: top">9892</td>
        <td style="vertical-align: top">Any integer representing valid port</td>
    </tr>
    <tr>
        <td style="vertical-align: top">receiver.threads</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">Number of threads to receive connections.</p></td>
        <td style="vertical-align: top">10</td>
        <td style="vertical-align: top">Any positive integer</td>
    </tr>
    <tr>
        <td style="vertical-align: top">worker.threads</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">Number of threads to serve events.</p></td>
        <td style="vertical-align: top">10</td>
        <td style="vertical-align: top">Any positive integer</td>
    </tr>
    <tr>
        <td style="vertical-align: top">tcp.no.delay</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">This is to specify whether to disable Nagle algorithm during message passing.<br>If tcp.no.delay = 'true', the execution of Nagle algorithm  will be disabled in the underlying TCP logic. Hence there will be no delay between two successive writes to the TCP connection.<br>Else there can be a constant ack delay.</p></td>
        <td style="vertical-align: top">true</td>
        <td style="vertical-align: top">true<br>false</td>
    </tr>
    <tr>
        <td style="vertical-align: top">keep.alive</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">This property defines whether the server should be kept alive when there are no connections available.</p></td>
        <td style="vertical-align: top">true</td>
        <td style="vertical-align: top">true<br>false</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@Source(type = 'tcp', context='abc', @map(type='binary'))
define stream Foo (attribute1 string, attribute2 int );
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Under this configuration, events are received via the TCP transport on default host,port, <code>abc</code> context, and they are passed to <code>Foo</code> stream for processing. </p>
<p></p>
