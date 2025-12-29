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

import static net.greghaines.jesque.utils.ResqueConstants.COLON;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.meta.KeyInfo;
import net.greghaines.jesque.meta.KeyType;
import net.greghaines.jesque.meta.dao.KeysDAO;
import redis.clients.jedis.UnifiedJedis;

/**
 * KeysDAORedisImpl gets key information from Redis.
 *
 * @author Greg Haines
 */
public class KeysDAORedisImpl implements KeysDAO {

  private static final Pattern NEW_LINE_PATTERN = Pattern.compile("\r\n");
  private static final Pattern COLON_PATTERN = Pattern.compile(":");

  private final Config config;
  private final UnifiedJedis jedisPool;

  /**
   * Constructor.
   *
   * @param config the Jesque configuration
   * @param jedisPool the pool of Jedis connections
   */
  public KeysDAORedisImpl(final Config config, final UnifiedJedis jedisPool) {
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
  public KeyInfo getKeyInfo(final String key) {
    try {
      return new KeyDAOWork(key).doWork(this.jedisPool);
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public KeyInfo getKeyInfo(final String key, final int offset, final int count) {
    try {
      return new KeyDAOWork(key, offset, count).doWork(this.jedisPool);
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public List<KeyInfo> getKeyInfos() {
    try {
      final Set<String> keys =
          this.jedisPool.keys(KeysDAORedisImpl.this.config.getNamespace() + COLON + "*");
      final List<KeyInfo> keyInfos = new ArrayList<>(keys.size());
      for (final String key : keys) {
        keyInfos.add(new KeyDAOWork(key).doWork(this.jedisPool));
      }
      Collections.sort(keyInfos);
      return keyInfos;
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, String> getRedisInfo() {
    try {
      final Map<String, String> infoMap = new TreeMap<>();
      final String infoStr = this.jedisPool.info();
      final String[] keyValueStrs = NEW_LINE_PATTERN.split(infoStr);
      for (final String keyValueStr : keyValueStrs) {
        if (!keyValueStr.isEmpty() && keyValueStr.charAt(0) != '#') {
          // Ignore categories for now
          final String[] keyAndValue = COLON_PATTERN.split(keyValueStr, 2);
          final String value = (keyAndValue.length == 1) ? null : keyAndValue[1];
          infoMap.put(keyAndValue[0], value);
        }
      }
      return new LinkedHashMap<>(infoMap);
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected static final class KeyDAOWork {

    private final String key;
    private final int offset;
    private final int count;
    private final boolean doArrayValue;

    protected KeyDAOWork(final String key) {
      this.key = key;
      this.offset = -1;
      this.count = -1;
      this.doArrayValue = false;
    }

    protected KeyDAOWork(final String key, final int offset, final int count) {
      this.key = key;
      this.offset = offset;
      this.count = count;
      this.doArrayValue = true;
    }

    public KeyInfo doWork(final UnifiedJedis jedis) throws Exception {
      final KeyInfo keyInfo;
      final KeyType type = KeyType.getKeyTypeByValue(jedis.type(this.key));
      if (type == null) {
        keyInfo = null;
      } else {
        switch (type) {
          case HASH:
            keyInfo = handleHash(jedis);
            break;
          case LIST:
            keyInfo = handleList(jedis);
            break;
          case SET:
            keyInfo = handleSet(jedis);
            break;
          case STRING:
            keyInfo = handleString(jedis);
            break;
          case ZSET:
            keyInfo = handleZSet(jedis);
            break;
          default: // NONE
            keyInfo = null;
            break;
        }
      }
      return keyInfo;
    }

    protected KeyInfo handleZSet(final UnifiedJedis jedis) {
      final KeyInfo keyInfo = new KeyInfo(this.key, KeyType.ZSET);
      keyInfo.setSize(jedis.zcard(this.key));
      if (this.doArrayValue) {
        keyInfo.setArrayValue(
            new ArrayList<String>(jedis.zrange(this.key, this.offset, this.offset + this.count)));
      }
      return keyInfo;
    }

    protected KeyInfo handleString(final UnifiedJedis jedis) {
      final KeyInfo keyInfo = new KeyInfo(this.key, KeyType.STRING);
      keyInfo.setSize(jedis.strlen(this.key));
      if (this.doArrayValue) {
        final List<String> arrayValue = new ArrayList<String>(1);
        arrayValue.add(jedis.get(this.key));
        keyInfo.setArrayValue(arrayValue);
      }
      return keyInfo;
    }

    protected KeyInfo handleSet(final UnifiedJedis jedis) {
      final KeyInfo keyInfo = new KeyInfo(this.key, KeyType.SET);
      keyInfo.setSize(jedis.scard(this.key));
      if (this.doArrayValue) {
        final List<String> allMembers = new ArrayList<String>(jedis.smembers(this.key));
        if (this.offset >= allMembers.size()) {
          keyInfo.setArrayValue(new ArrayList<String>(1));
        } else {
          final int toIndex =
              (this.offset + this.count > allMembers.size())
                  ? allMembers.size()
                  : (this.offset + this.count);
          keyInfo.setArrayValue(new ArrayList<String>(allMembers.subList(this.offset, toIndex)));
        }
      }
      return keyInfo;
    }

    protected KeyInfo handleList(final UnifiedJedis jedis) {
      final KeyInfo keyInfo = new KeyInfo(this.key, KeyType.LIST);
      keyInfo.setSize(jedis.llen(this.key));
      if (this.doArrayValue) {
        keyInfo.setArrayValue(jedis.lrange(this.key, this.offset, this.offset + this.count));
      }
      return keyInfo;
    }

    protected KeyInfo handleHash(final UnifiedJedis jedis) {
      final KeyInfo keyInfo = new KeyInfo(this.key, KeyType.HASH);
      keyInfo.setSize(jedis.hlen(this.key));
      if (this.doArrayValue) {
        final List<String> allFields = new ArrayList<String>(jedis.hkeys(this.key));
        if (this.offset >= allFields.size()) {
          keyInfo.setArrayValue(new ArrayList<String>(1));
        } else {
          final int toIndex =
              (this.offset + this.count > allFields.size())
                  ? allFields.size()
                  : (this.offset + this.count);
          final List<String> subFields = allFields.subList(this.offset, toIndex);
          final List<String> values =
              jedis.hmget(this.key, subFields.toArray(new String[subFields.size()]));
          final List<String> arrayValue = new ArrayList<String>(subFields.size());
          for (int i = 0; i < subFields.size(); i++) {
            arrayValue.add("{" + subFields.get(i) + "=" + values.get(i) + "}");
          }
          keyInfo.setArrayValue(arrayValue);
        }
      }
      return keyInfo;
    }
  }
}
