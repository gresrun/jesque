Jesque
======

Jesque is an implementation of [Resque](https://github.com/defunkt/resque) in [Java](http://www.oracle.com/technetwork/java/index.html). It is fully-interoperable with the [Ruby](http://www.ruby-lang.org/en/) and [Node.js](http://nodejs.org/) ([Coffee-Resque](https://github.com/technoweenie/coffee-resque)) implementations.

Jesque is a [Maven](http://maven.apache.org/) project and depends on [Jedis](https://github.com/xetorthio/jedis) to connect to [Redis](http://redis.io/), [Jackson](http://jackson.codehaus.org/) to map to/from [JSON](http://www.json.org/) and [SLF4J](http://www.slf4j.org/) for logging.

The project contains a client implementation as well as a worker implementation that supports listeners.

***

How do I use it?
----------------
Download the latest source at:

	https://github.com/gresrun/jesque
Or, to use it in your Maven project, add it as a dependency:

```xml
<dependency>
	<groupId>net.greghaines</groupId>
	<artifactId>jesque</artifactId>
	<version>1.0.0</version>
	<type>jar</type>
	<scope>compile</scope>
</dependency>
```

Example usage (from IntegrationTest):

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
	Arrays.asList("foo"), map(entry("TestAction", TestAction.class)));
final Thread workerThread = new Thread(worker);
workerThread.start();

// Wait a few secs then shutdown
try { Thread.sleep(5000); } catch (Exception e){} // Give ourselves time to process
worker.end(true);
try { workerThread.join(); } catch (Exception e){ e.printStackTrace(); }
```

For more usage examples check the tests. The tests require that Redis is running on localhost:6379.

Use the resque-web application to see the status of your jobs and workers or, if you prefer Java, try [Jesque-Web](https://github.com/gresrun/jesque-web).

***

Design Decisions
----------------
* I chose to implement the jobs as classes that implement `java.lang.Runnable` or `java.util.concurrent.Callable`. If the job requires arguments (most do), there must be a constructor that matches the supplied arguments. I felt this was the most flexible option and didn't require the jobs to inherit or implement a special Jesque class. Because of this, the jobs don't even need to know about Jesque at all. Furthermore, the client need not have the job's `Class` in it's VM, it only needs to know the classname and all the parameters' `Class`es on it's classpath. Only the workers realize the job and then run them.
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

Misc.
-----

If you are on Mac OS X, I highly recommend using the fantasic [Homebrew package manager](https://github.com/mxcl/homebrew). It makes installing and maintaining libraries, tools and applications a cinch. E.g.:

```bash
brew install redis
brew install git
brew install maven
gem install resque
```

Boom! Ready to go!

***

License
-------
Copyright 2011 Greg Haines

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   <http://www.apache.org/licenses/LICENSE-2.0>

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
