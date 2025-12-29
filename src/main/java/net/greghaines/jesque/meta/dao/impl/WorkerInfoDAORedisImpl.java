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
package net.greghaines.jesque.meta.dao.impl;

import static net.greghaines.jesque.utils.ResqueConstants.FAILED;
import static net.greghaines.jesque.utils.ResqueConstants.PROCESSED;
import static net.greghaines.jesque.utils.ResqueConstants.STARTED;
import static net.greghaines.jesque.utils.ResqueConstants.STAT;
import static net.greghaines.jesque.utils.ResqueConstants.WORKER;
import static net.greghaines.jesque.utils.ResqueConstants.WORKERS;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.WorkerStatus;
import net.greghaines.jesque.json.ObjectMapperFactory;
import net.greghaines.jesque.meta.WorkerInfo;
import net.greghaines.jesque.meta.dao.WorkerInfoDAO;
import net.greghaines.jesque.utils.CompositeDateFormat;
import net.greghaines.jesque.utils.JesqueUtils;
import redis.clients.jedis.UnifiedJedis;

/**
 * WorkerInfoDAORedisImpl gets worker information from Redis.
 *
 * @author Greg Haines
 */
public class WorkerInfoDAORedisImpl implements WorkerInfoDAO {

  private static final Pattern COLON_PATTERN = Pattern.compile(":");
  private static final Pattern COMMA_PATTERN = Pattern.compile(",");

  private final Config config;
  private final UnifiedJedis jedisPool;

  /**
   * Constructor.
   *
   * @param config the Jesque configuration
   * @param jedisPool the pool of Jedis connections
   */
  public WorkerInfoDAORedisImpl(final Config config, final UnifiedJedis jedisPool) {
    if (config == null) {
      throw new IllegalArgumentException("config must not be null");
    }
    if (jedisPool == null) {
      throw new IllegalArgumentException("jedisPool must not be null");
    }
    this.config = config;
    this.jedisPool = jedisPool;
  }

  /** {@inheritDoc} */
  @Override
  public long getWorkerCount() {
    return this.jedisPool.scard(key(WORKERS));
  }

  /** {@inheritDoc} */
  @Override
  public long getActiveWorkerCount() {
    try {
      long activeCount = 0L;
      final Set<String> workerNames = this.jedisPool.smembers(key(WORKERS));
      for (final String workerName : workerNames) {
        if (isWorkerInState(workerName, WorkerInfo.State.WORKING, this.jedisPool)) {
          activeCount++;
        }
      }
      return activeCount;
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public long getPausedWorkerCount() {
    try {
      long pausedCount = 0L;
      final Set<String> workerNames = this.jedisPool.smembers(key(WORKERS));
      for (final String workerName : workerNames) {
        if (isWorkerInState(workerName, WorkerInfo.State.PAUSED, this.jedisPool)) {
          pausedCount++;
        }
      }
      return pausedCount;
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public List<WorkerInfo> getActiveWorkers() {
    return getWorkerInfos(WorkerInfo.State.WORKING);
  }

  /** {@inheritDoc} */
  @Override
  public List<WorkerInfo> getPausedWorkers() {
    return getWorkerInfos(WorkerInfo.State.PAUSED);
  }

  /** {@inheritDoc} */
  @Override
  public List<WorkerInfo> getAllWorkers() {
    return getWorkerInfos(null);
  }

  private List<WorkerInfo> getWorkerInfos(final WorkerInfo.State requestedState) {
    try {
      final Set<String> workerNames = this.jedisPool.smembers(key(WORKERS));
      final List<WorkerInfo> workerInfos = new ArrayList<WorkerInfo>(workerNames.size());
      for (final String workerName : workerNames) {
        if (isWorkerInState(workerName, requestedState, this.jedisPool)) {
          workerInfos.add(createWorker(workerName, this.jedisPool));
        }
      }
      Collections.sort(workerInfos);
      return workerInfos;
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public WorkerInfo getWorker(final String workerName) {
    try {
      WorkerInfo workerInfo = null;
      if (this.jedisPool.sismember(key(WORKERS), workerName)) {
        workerInfo = createWorker(workerName, this.jedisPool);
      }
      return workerInfo;
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, List<WorkerInfo>> getWorkerHostMap() {
    final List<WorkerInfo> workerInfos = getAllWorkers();
    final Map<String, List<WorkerInfo>> hostMap = new TreeMap<String, List<WorkerInfo>>();
    for (final WorkerInfo workerInfo : workerInfos) {
      List<WorkerInfo> hostWIs = hostMap.get(workerInfo.getHost());
      if (hostWIs == null) {
        hostWIs = new ArrayList<WorkerInfo>();
        hostMap.put(workerInfo.getHost(), hostWIs);
      }
      hostWIs.add(workerInfo);
    }
    return hostMap;
  }

  /**
   * Builds a namespaced Redis key with the given arguments.
   *
   * @param parts the key parts to be joined
   * @return an assembled String key
   */
  private String key(final String... parts) {
    return JesqueUtils.createKey(this.config.getNamespace(), parts);
  }

  protected WorkerInfo createWorker(final String workerName, final UnifiedJedis jedis)
      throws ParseException, IOException {
    final WorkerInfo workerInfo = new WorkerInfo();
    workerInfo.setName(workerName);
    final String[] nameParts = COLON_PATTERN.split(workerName, 3);
    if (nameParts.length < 3) {
      throw new ParseException("Malformed worker name: " + workerName, 0);
    }
    workerInfo.setHost(nameParts[0]);
    workerInfo.setPid(nameParts[1]);
    workerInfo.setQueues(new ArrayList<String>(Arrays.asList(COMMA_PATTERN.split(nameParts[2]))));
    final String statusPayload = jedis.get(key(WORKER, workerName));
    if (statusPayload != null) {
      workerInfo.setStatus(ObjectMapperFactory.get().readValue(statusPayload, WorkerStatus.class));
      final WorkerInfo.State state =
          (workerInfo.getStatus().isPaused()) ? WorkerInfo.State.PAUSED : WorkerInfo.State.WORKING;
      workerInfo.setState(state);
    } else {
      workerInfo.setState(WorkerInfo.State.IDLE);
    }
    final String startedStr = jedis.get(key(WORKER, workerName, STARTED));
    if (startedStr != null) {
      workerInfo.setStarted(new CompositeDateFormat().parse(startedStr));
    }
    final String failedStr = jedis.get(key(STAT, FAILED, workerName));
    if (failedStr != null) {
      workerInfo.setFailed(Long.parseLong(failedStr));
    } else {
      workerInfo.setFailed(0L);
    }
    final String processedStr = jedis.get(key(STAT, PROCESSED, workerName));
    if (processedStr != null) {
      workerInfo.setProcessed(Long.parseLong(processedStr));
    } else {
      workerInfo.setProcessed(0L);
    }
    return workerInfo;
  }

  /** {@inheritDoc} */
  @Override
  public void removeWorker(final String workerName) {
    this.jedisPool.srem(key(WORKERS), workerName);
    this.jedisPool.del(
        key(WORKER, workerName),
        key(WORKER, workerName, STARTED),
        key(STAT, FAILED, workerName),
        key(STAT, PROCESSED, workerName));
  }

  protected boolean isWorkerInState(
      final String workerName, final WorkerInfo.State requestedState, final UnifiedJedis jedis)
      throws IOException {
    boolean proceed = true;
    if (requestedState != null) {
      final String statusPayload = jedis.get(key(WORKER, workerName));
      final WorkerStatus status =
          (statusPayload == null)
              ? null
              : ObjectMapperFactory.get().readValue(statusPayload, WorkerStatus.class);
      switch (requestedState) {
        case IDLE:
          proceed = (status == null);
          break;
        case PAUSED:
          proceed = (status == null) ? true : status.isPaused();
          break;
        case WORKING:
          proceed = (status == null) ? false : !status.isPaused();
          break;
      }
    }
    return proceed;
  }
}
