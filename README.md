# Jesque

[![Build Status](http://img.shields.io/travis/gresrun/jesque.svg)](https://travis-ci.org/gresrun/jesque) [![Coverage Status](http://img.shields.io/coveralls/gresrun/jesque.svg)](https://coveralls.io/r/gresrun/jesque) [![License Apache 2.0](http://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/gresrun/jesque/blob/master/LICENSE) [![Maven Central](https://img.shields.io/maven-central/v/net.greghaines/jesque.svg)](http://search.maven.org/#search%7Cgav%7C1%7Cg%3A%22net.greghaines%22%20AND%20a%3A%22jesque%22)

Jesque is an implementation of [Resque](https://github.com/resque/resque) in [Java](http://www.oracle.com/technetwork/java/index.html). It is fully-interoperable with the [Ruby](http://www.ruby-lang.org/en/) and [Node.js](http://nodejs.org/) ([Coffee-Resque](https://github.com/technoweenie/coffee-resque)) implementations.

Jesque is a [Maven](http://maven.apache.org/) project and depends on [Jedis](https://github.com/xetorthio/jedis) to connect to [Redis](http://redis.io/), [Jackson](https://github.com/FasterXML/jackson) to map to/from [JSON](http://json.org/) and [SLF4J](http://slf4j.org/) for logging.

The project contains a client implementation as well as a worker implementation that supports listeners.

*NOTE:* Jesque's delayed jobs implementation is not compatible with resque-scheduler

***

## How do I use it?

Jesque requires Java 7+. Download the latest source at:

  https://github.com/gresrun/jesque
Or, to use it in your Maven project, add it as a dependency:

```xml
<dependency>
  <groupId>net.greghaines</groupId>
  <artifactId>jesque</artifactId>
  <version>2.1.2</version>
</dependency>
```

## Quickstart:

```java
// Configuration
final Config config = new ConfigBuilder().build();

// Add a job to the queue
final Job job = new Job("TestAction",
  new Object[]{ 1, 2.3, true, "test", Arrays.asList("inner", 4.5)});
final Client client = new ClientImpl(config);
client.enqueue("foo", job);
client.end();

// Start a worker to run jobs from the queue
final Worker worker = new WorkerImpl(config,
  Arrays.asList("foo"), new MapBasedJobFactory(map(entry("TestAction", TestAction.class))));
  
final Thread workerThread = new Thread(worker);
workerThread.start();

// Enqueue more jobs, etc.

// Shutdown the worker when finished
worker.end(true);
try { workerThread.join(); } catch (Exception e){ e.printStackTrace(); }
```

### Delayed jobs
Delayed jobs can be executed at sometime in the future.
```java
final long delay = 10; // in seconds
final long future = System.currentTimeMillis() + (delay * 1000); // timestamp

client.delayedEnqueue("fooDelay", job, future);
```

### Recurring Jobs
Recurring jobs can start at a specific time and execute at specified intervals. 
```java
final long delay = 10; // in seconds
final long future = System.currentTimeMillis() + (delay * 1000); // timestamp
final long frequency = 60; // in seconds

client.recurringEnqueue("fooRecur", job, future, (frequency * 1000));
```

### Cancelling jobs
Delayed and recurring jobs can be cancelled.
```java
client.removeDelayedEnqueue("fooDelay", job);
client.removeRecurringEnqueue("fooRecur", job);
```

### Using a ClientPool
`ClientPool` is useful in multi threaded apps, 
```java
final Client jesqueClientPool = new ClientPoolImpl(config, PoolUtils.createJedisPool(config));
jesqueClientPool.enqueue("foo", job);
```

### Listeners
You can execute custom callbacks during specific Worker events.
```java
int myVar = 0;
worker.getWorkerEventEmitter().addListener(new WorkerListener(){
   public void onEvent(WorkerEvent event, Worker worker, String queue, Job job, 
					   Object runner, Object result, Throwable t) {
    if (runner instanceof TestAction) {
        ((TestAction) runner).setSomeVariable(myVar);
    }
  }
}, WorkerEvent.JOB_EXECUTE);
```

#### Available Worker Events

* `WORKER_START` Finished starting up and is about to start running.
* `WORKER_POLL` Polling the queue.
* `JOB_PROCESS` Processing a Job.
* `JOB_EXECUTE` About to execute a materialized Job.
* `JOB_SUCCESS` Successfully executed a materialized Job.
* `JOB_FAILURE` Caught an Exception during the execution of a materialized Job.
* `WORKER_ERROR` Caught an Exception during normal operation.
* `WORKER_STOP` Finished running and is about to shutdown.

For more usage examples check the tests. The tests require that Redis is running on `localhost:6379`.

Use the resque-web application to see the status of your jobs and workers or, if you prefer Java, try [Jesque-Web](https://github.com/gresrun/jesque-web).

### Redis Configuration

As mentioned Jesque depends on [Jedis](https://github.com/xetorthio/jedis) to connect to [Redis](http://redis.io/).

You can configure Jesque to connect to Redis given a URL in a system property (as used in Heroku + RedisToGo) with the following snippet:

```java
final ConfigBuilder configBuilder = new ConfigBuilder();
try {
  URI redisUrl = new URI(System.getProperty("REDIS_PROVIDER", "127.0.0.1"));
  String redisHost = redisUrl.getHost();
  int redisPort = redisUrl.getPort();
  String redisUserInfo = redisUrl.getUserInfo();
  if (redisHost != null) {
    configBuilder.withHost(redisHost);
  }
  if (redisPort > -1) {
    configBuilder.withPort(redisPort);
  }
  if (redisUserInfo != null) {
    configBuilder.withPassword(redisUserInfo.split(":",2)[1]);
  }
} catch (URISyntaxException use) {
  // Handle error
}
final Config config = configBuilder.build();
```

***

## Design Decisions

* I chose to implement the jobs as classes that implement `java.lang.Runnable` or `java.util.concurrent.Callable`. If the job requires arguments (most do), there must be a constructor that matches the supplied arguments. I felt this was the most flexible option and didn't require the jobs to inherit or implement a special Jesque class. Because of this, the jobs don't even need to know about Jesque at all. Furthermore, the client need not have the job's `Class` in its VM, it only needs to know the classname and all the parameters' `Class`es on its classpath. Only the workers realize the job and then run them.
* I chose to use Jedis because:
  1. It is simple to use
  2. Fully supports Redis 2.0 and uses the new unified protocol
  3. No dependencies
* I chose to use Jackson because:
  1. I was already familiar with it
  2. It performs great and does what it says on the tin
  3. No dependencies
* I chose to use SLF4J because:
  1. It lets the application choose how to log
  2. No dependencies

***

## Misc.

If you are on Mac OS X, I highly recommend using the fantasic [Homebrew package manager](https://github.com/mxcl/homebrew). It makes installing and maintaining libraries, tools and applications a cinch. E.g.:

```bash
brew install redis
brew install git
brew install maven
```

Boom! Ready to go!

***

## License

Copyright 2015 Greg Haines

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   <http://www.apache.org/licenses/LICENSE-2.0>

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
