Siddhi IO TCP
===================

  [![Jenkins Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-io-tcp/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-io-tcp/)
  [![GitHub Release](https://img.shields.io/github/release/siddhi-io/siddhi-io-tcp.svg)](https://github.com/siddhi-io/siddhi-io-tcp/releases)
  [![GitHub Release Date](https://img.shields.io/github/release-date/siddhi-io/siddhi-io-tcp.svg)](https://github.com/siddhi-io/siddhi-io-tcp/releases)
  [![GitHub Open Issues](https://img.shields.io/github/issues-raw/siddhi-io/siddhi-io-tcp.svg)](https://github.com/siddhi-io/siddhi-io-tcp/issues)
  [![GitHub Last Commit](https://img.shields.io/github/last-commit/siddhi-io/siddhi-io-tcp.svg)](https://github.com/siddhi-io/siddhi-io-tcp/commits/master)
  [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

The **siddhi-io-tcp extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a> that receives and publishes events through TCP transport.

For information on <a target="_blank" href="https://siddhi.io/">Siddhi</a> and it's features refer <a target="_blank" href="https://siddhi.io/redirect/docs.html">Siddhi Documentation</a>. 

## Download

* Versions 3.x and above with group id `io.siddhi.extension.*` from <a target="_blank" href="https://mvnrepository.com/artifact/io.siddhi.extension.io.tcp/siddhi-io-tcp/">here</a>.
* Versions 2.x and lower with group id `org.wso2.extension.siddhi.*` from <a target="_blank" href="https://mvnrepository.com/artifact/org.wso2.extension.siddhi.io.tcp/siddhi-io-tcp">here</a>.

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-tcp/api/3.0.5">3.0.5</a>.

## Features

* <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-tcp/api/3.0.5/#tcp-sink">tcp</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#sink">Sink</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">A Siddhi application can be configured to publish events via the TCP transport by adding the @Sink(type = 'tcp') annotation at the top of an event stream definition.</p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-tcp/api/3.0.5/#tcp-source">tcp</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#source">Source</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">A Siddhi application can be configured to receive events via the TCP transport by adding the @Source(type = 'tcp') annotation at the top of an event stream definition.<br><br>When this is defined the associated stream will receive events from the TCP transport on the host and port defined in the system.</p></p></div>

## Dependencies 

There are no other dependencies needed for this extension. 

## Installation

For installing this extension on various siddhi execution environments refer Siddhi documentation section on <a target="_blank" href="https://siddhi.io/redirect/add-extensions.html">adding extensions</a>.

## Support and Contribution

* We encourage users to ask questions and get support via <a target="_blank" href="https://stackoverflow.com/questions/tagged/siddhi">StackOverflow</a>, make sure to add the `siddhi` tag to the issue for better response.

* If you find any issues related to the extension please report them on <a target="_blank" href="https://github.com/siddhi-io/siddhi-execution-string/issues">the issue tracker</a>.

* For production support and other contribution related information refer <a target="_blank" href="https://siddhi.io/community/">Siddhi Community</a> documentation.
