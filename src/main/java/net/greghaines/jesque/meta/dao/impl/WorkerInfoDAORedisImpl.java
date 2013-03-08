/*
 * Copyright 2011 Greg Haines
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import net.greghaines.jesque.utils.PoolUtils;
import net.greghaines.jesque.utils.PoolUtils.PoolWork;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

public class WorkerInfoDAORedisImpl implements WorkerInfoDAO
{
	private static final Pattern colonPattern = Pattern.compile(":");
	private static final Pattern commaPattern = Pattern.compile(",");
	
	private final Config config;
	private final Pool<Jedis> jedisPool;
	
	public WorkerInfoDAORedisImpl(final Config config, final Pool<Jedis> jedisPool)
	{
		if (config == null)
		{
			throw new IllegalArgumentException("config must not be null");
		}
		if (jedisPool == null)
		{
			throw new IllegalArgumentException("jedisPool must not be null");
		}
		this.config = config;
		this.jedisPool = jedisPool;
	}
	
	public long getWorkerCount()
	{
		return PoolUtils.doWorkInPoolNicely(this.jedisPool, new PoolWork<Jedis,Long>()
		{
			public Long doWork(final Jedis jedis)
			throws Exception
			{
				return jedis.scard(key(WORKERS));
			}
		});
	}
	
	public long getActiveWorkerCount()
	{
		return PoolUtils.doWorkInPoolNicely(this.jedisPool, new PoolWork<Jedis,Long>()
		{
			public Long doWork(final Jedis jedis)
			throws Exception
			{
				long activeCount = 0L;
				final Set<String> workerNames = jedis.smembers(key(WORKERS));
				for (final String workerName : workerNames)
				{
					if (isWorkerInState(workerName, WorkerInfo.State.WORKING, jedis))
					{
						activeCount++;
					}
				}
				return activeCount;
			}
		});
	}
	
	public long getPausedWorkerCount()
	{
		return PoolUtils.doWorkInPoolNicely(this.jedisPool, new PoolWork<Jedis,Long>()
		{
			public Long doWork(final Jedis jedis)
			throws Exception
			{
				long pausedCount = 0L;
				final Set<String> workerNames = jedis.smembers(key(WORKERS));
				for (final String workerName : workerNames)
				{
					if (isWorkerInState(workerName, WorkerInfo.State.PAUSED, jedis))
					{
						pausedCount++;
					}
				}
				return pausedCount;
			}
		});
	}

	public List<WorkerInfo> getActiveWorkers()
	{
		return getWorkerInfos(WorkerInfo.State.WORKING);
	}

	public List<WorkerInfo> getPausedWorkers()
	{
		return getWorkerInfos(WorkerInfo.State.PAUSED);
	}
	
	public List<WorkerInfo> getAllWorkers()
	{
		return getWorkerInfos(null);
	}
	
	private List<WorkerInfo> getWorkerInfos(final WorkerInfo.State requestedState)
	{
		return PoolUtils.doWorkInPoolNicely(this.jedisPool, new PoolWork<Jedis,List<WorkerInfo>>()
		{
			public List<WorkerInfo> doWork(final Jedis jedis)
			throws Exception
			{
				final Set<String> workerNames = jedis.smembers(key(WORKERS));
				final List<WorkerInfo> workerInfos = new ArrayList<WorkerInfo>(workerNames.size());
				for (final String workerName : workerNames)
				{
					if (isWorkerInState(workerName, requestedState, jedis))
					{
						workerInfos.add(createWorker(workerName, jedis));
					}
				}
				Collections.sort(workerInfos);
				return workerInfos;
			}
		});
	}
	
	public WorkerInfo getWorker(final String workerName)
	{
		return PoolUtils.doWorkInPoolNicely(this.jedisPool, new PoolWork<Jedis,WorkerInfo>()
		{
			public WorkerInfo doWork(final Jedis jedis)
			throws Exception
			{
				WorkerInfo workerInfo = null;
				if (jedis.sismember(key(WORKERS), workerName))
				{
					workerInfo = createWorker(workerName, jedis);
				}
				return workerInfo;
			}
		});
	}
	
	public Map<String,List<WorkerInfo>> getWorkerHostMap()
	{
		final List<WorkerInfo> workerInfos = getAllWorkers();
		final Map<String,List<WorkerInfo>> hostMap = new TreeMap<String,List<WorkerInfo>>();
		for (final WorkerInfo workerInfo : workerInfos)
		{
			List<WorkerInfo> hostWIs = hostMap.get(workerInfo.getHost());
			if (hostWIs == null)
			{
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
	private String key(final String... parts)
	{
		return JesqueUtils.createKey(this.config.getNamespace(), parts);
	}

	private WorkerInfo createWorker(final String workerName, final Jedis jedis)
	throws ParseException, IOException
	{
		final WorkerInfo workerInfo = new WorkerInfo();
		workerInfo.setName(workerName);
		final String[] nameParts = colonPattern.split(workerName, 3);
		if (nameParts.length < 3)
		{
			throw new ParseException("Malformed worker name: " + workerName, 0);
		}
		workerInfo.setHost(nameParts[0]);
		workerInfo.setPid(nameParts[1]);
		workerInfo.setQueues(new ArrayList<String>(Arrays.asList(commaPattern.split(nameParts[2]))));
		final String statusPayload = jedis.get(key(WORKER, workerName));
		if (statusPayload != null)
		{
			workerInfo.setStatus(ObjectMapperFactory.get().readValue(statusPayload, WorkerStatus.class));
			final WorkerInfo.State state = (workerInfo.getStatus().isPaused())
				? WorkerInfo.State.PAUSED
				: WorkerInfo.State.WORKING;
			workerInfo.setState(state);
		}
		else
		{
			workerInfo.setState(WorkerInfo.State.IDLE);
		}
		final String startedStr = jedis.get(key(WORKER, workerName, STARTED));
		if (startedStr != null)
		{
			workerInfo.setStarted(new CompositeDateFormat().parse(startedStr));
		}
		final String failedStr = jedis.get(key(STAT, FAILED, workerName));
		if (failedStr != null)
		{
			workerInfo.setFailed(Long.parseLong(failedStr));
		}
		else
		{
			workerInfo.setFailed(0L);
		}
		final String processedStr = jedis.get(key(STAT, PROCESSED, workerName));
		if (processedStr != null)
		{
			workerInfo.setProcessed(Long.parseLong(processedStr));
		}
		else
		{
			workerInfo.setProcessed(0L);
		}
		return workerInfo;
	}

	/**
	 * Remove the metadata about a worker
	 *
	 * @param workerName The worker name to remove
	 */
	public void removeWorker(final String workerName)
	{
		PoolUtils.doWorkInPoolNicely(this.jedisPool, new PoolWork<Jedis,Void>()
		{
			public Void doWork(final Jedis jedis)
			throws Exception
			{
				jedis.srem(key(WORKERS), workerName);
				jedis.del(
					key(WORKER, workerName),
					key(WORKER, workerName, STARTED),
					key(STAT, FAILED, workerName),
					key(STAT, PROCESSED, workerName));
				return null;
			}
		});
	}
	
	private boolean isWorkerInState(final String workerName, final WorkerInfo.State requestedState, 
			final Jedis jedis)
	throws IOException
	{
		boolean proceed = true;
		final String statusPayload = jedis.get(key(WORKER, workerName));
		if (requestedState != null)
		{
			switch (requestedState)
			{
			case IDLE: {
				proceed = (statusPayload == null);
				break;
			}
			case PAUSED: {
				if (statusPayload != null)
				{
					final WorkerStatus status = ObjectMapperFactory.get().readValue(statusPayload, WorkerStatus.class);
					proceed = status.isPaused();
				}
				break;
			}
			case WORKING: {
				if (statusPayload != null)
				{
					final WorkerStatus status = ObjectMapperFactory.get().readValue(statusPayload, WorkerStatus.class);
					proceed = !status.isPaused();
				}
				break;
			}
			}
		}
		return proceed;
	}
}
