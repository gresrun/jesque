/*
 * Copyright 2011 Greg Haines
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package net.greghaines.jesque.client;

import java.util.List;
import net.greghaines.jesque.Config;
import redis.clients.jedis.UnifiedJedis;

/**
 * A Client implementation that gets its connection to Redis from a connection pool.
 *
 * @author Greg Haines
 */
public class ClientPoolImpl extends AbstractClient {

  private final UnifiedJedis jedisPool;

  /**
   * Create a ClientPoolImpl.
   *
   * @param config used to get the namespace for key creation
   * @param jedisPool the connection pool
   */
  public ClientPoolImpl(final Config config, final UnifiedJedis jedisPool) {
    super(config);
    if (jedisPool == null) {
      throw new IllegalArgumentException("jedisPool must not be null");
    }
    this.jedisPool = jedisPool;
  }

  /** {@inheritDoc} */
  @Override
  protected void doEnqueue(final String queue, final String jobJson) throws Exception {
    doEnqueue(this.jedisPool, getNamespace(), queue, jobJson);
  }

  @Override
  protected void doBatchEnqueue(final String queue, final List<String> jobsJson) throws Exception {
    doBatchEnqueue(this.jedisPool, this.jedisPool::pipelined, getNamespace(), queue, jobsJson);
  }

  /** {@inheritDoc} */
  @Override
  protected void doPriorityEnqueue(final String queue, final String jobJson) throws Exception {
    doPriorityEnqueue(this.jedisPool, getNamespace(), queue, jobJson);
  }

  /** {@inheritDoc} */
  @Override
  protected boolean doAcquireLock(final String lockName, final String lockHolder, final int timeout)
      throws Exception {
    return doAcquireLock(this.jedisPool, getNamespace(), lockName, lockHolder, timeout);
  }

  /** {@inheritDoc} */
  @Override
  public void end() {
    // Do nothing
  }

  /** {@inheritDoc} */
  @Override
  protected void doDelayedEnqueue(final String queue, final String msg, final long future)
      throws Exception {
    doDelayedEnqueue(this.jedisPool, getNamespace(), queue, msg, future);
  }

  /** {@inheritDoc} */
  @Override
  protected void doRemoveDelayedEnqueue(final String queue, final String msg) throws Exception {
    doRemoveDelayedEnqueue(this.jedisPool, getNamespace(), queue, msg);
  }

  @Override
  protected void doRecurringEnqueue(
      final String queue, final String msg, final long future, final long frequency)
      throws Exception {
    doRecurringEnqueue(
        this.jedisPool, this.jedisPool::multi, getNamespace(), queue, msg, future, frequency);
  }

  @Override
  protected void doRemoveRecurringEnqueue(final String queue, final String msg) throws Exception {
    doRemoveRecurringEnqueue(this.jedisPool, this.jedisPool::multi, getNamespace(), queue, msg);
  }
}
