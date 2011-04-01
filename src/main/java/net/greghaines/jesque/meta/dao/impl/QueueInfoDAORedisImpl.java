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

import static net.greghaines.jesque.utils.ResqueConstants.PROCESSED;
import static net.greghaines.jesque.utils.ResqueConstants.QUEUE;
import static net.greghaines.jesque.utils.ResqueConstants.QUEUES;
import static net.greghaines.jesque.utils.ResqueConstants.STAT;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.json.ObjectMapperFactory;
import net.greghaines.jesque.meta.QueueInfo;
import net.greghaines.jesque.meta.dao.QueueInfoDAO;
import net.greghaines.jesque.utils.JesqueUtils;
import net.greghaines.jesque.utils.PoolUtils;
import net.greghaines.jesque.utils.PoolUtils.PoolWork;

import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

public class QueueInfoDAORedisImpl implements QueueInfoDAO
{
	private final Config config;
	private final Pool<Jedis> jedisPool;
	
	public QueueInfoDAORedisImpl(final Config config, final Pool<Jedis> jedisPool)
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

	public List<String> getQueueNames()
	{
		return PoolUtils.doWorkInPoolNicely(this.jedisPool, new PoolWork<Jedis,List<String>>()
		{
			public List<String> doWork(final Jedis jedis)
			throws Exception
			{
				final List<String> queueNames = new ArrayList<String>(jedis.smembers(key(QUEUES)));
				Collections.sort(queueNames);
				return queueNames;
			}
		});
	}
	
	public long getPendingCount()
	{
		final List<String> queueNames = getQueueNames();
		return PoolUtils.doWorkInPoolNicely(this.jedisPool, new PoolWork<Jedis,Long>()
		{
			public Long doWork(final Jedis jedis)
			throws Exception
			{
				long pendingCount = 0L;
				for (final String queueName : queueNames)
				{
					pendingCount += jedis.llen(key(QUEUE, queueName));
				}
				return pendingCount;
			}
		});
	}
	
	public long getProcessedCount()
	{
		return PoolUtils.doWorkInPoolNicely(this.jedisPool, new PoolWork<Jedis,Long>()
		{
			public Long doWork(final Jedis jedis)
			throws Exception
			{
				final String processedStr = jedis.get(key(STAT, PROCESSED));
				return (processedStr == null) ? 0L : Long.parseLong(processedStr);
			}
		});
	}

	public List<QueueInfo> getQueueInfos()
	{
		final List<String> queueNames = getQueueNames();
		return PoolUtils.doWorkInPoolNicely(this.jedisPool, new PoolWork<Jedis,List<QueueInfo>>()
		{
			public List<QueueInfo> doWork(final Jedis jedis)
			throws Exception
			{
				final List<QueueInfo> queueInfos = new ArrayList<QueueInfo>(queueNames.size());
				for (final String queueName : queueNames)
				{
					final QueueInfo queueInfo = new QueueInfo();
					queueInfo.setName(queueName);
					queueInfo.setSize(jedis.llen(key(QUEUE, queueName)));
					queueInfos.add(queueInfo);
				}
				Collections.sort(queueInfos);
				return queueInfos;
			}
		});
	}

	public QueueInfo getQueueInfo(final String name, final int jobOffset, final int jobCount)
	{
		return PoolUtils.doWorkInPoolNicely(this.jedisPool, new PoolWork<Jedis,QueueInfo>()
		{
			public QueueInfo doWork(final Jedis jedis)
			throws Exception
			{
				final QueueInfo queueInfo = new QueueInfo();
				queueInfo.setName(name);
				queueInfo.setSize(jedis.llen(key(QUEUE, name)));
				final List<String> payloads = jedis.lrange(key(QUEUE, name), jobOffset, jobOffset + jobCount - 1);
				final List<Job> jobs = new ArrayList<Job>(payloads.size());
				for (final String payload : payloads)
				{
					jobs.add(ObjectMapperFactory.get().readValue(payload, Job.class));
				}
				queueInfo.setJobs(jobs);
				return queueInfo;
			}
		});
	}
	
	public void removeQueue(final String name)
	{
		PoolUtils.doWorkInPoolNicely(this.jedisPool, new PoolWork<Jedis,Void>()
		{
			public Void doWork(final Jedis jedis)
			throws Exception
			{
				jedis.srem(key(QUEUES), name);
				jedis.del(key(QUEUE, name));
				return null;
			}
		});
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
}
