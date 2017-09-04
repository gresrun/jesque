package net.greghaines.jesque.worker;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.utils.PoolUtils;
import net.greghaines.jesque.utils.ScriptUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.util.Pool;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;
import static net.greghaines.jesque.utils.PoolUtils.doWorkInPool;

/**
 * Created by dimav on 17/08/17.
 * <p>
 * service responsible to requeue lost jobs
 */
public class Watchdog {
    private static final Logger LOG = LoggerFactory.getLogger(Watchdog.class);
    private static final int POOL_SIZE = 2;
    private static final int INITIAL_DELAY = 0;
    private static final int LIGHT_KEEPER_PERIOD = 10;
    private static final int IS_ALIVE_PERIOD = 2;
    private static final int TIME_TO_REQUEUE_JOBS_ON_INACTIVE_SERVER_SEC = 60;
    private static final String WATCHDOG_LUA = "/workerScripts/watchdog.lua";
    private static final String REQUEUE_JOBS = "requeueJobs";
    private static final String IS_ALIVE = "isAlive";
    private static final int WATCHDOG_SCRIPT_KEYS_NUMBER = 0;
    private static final String WATCHDOG = "watchdog";
    private static final int REQUEUE_JOBS_RETRY_NUM = 3;
    private final Pool<Jedis> jedisPool;
    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(POOL_SIZE);
    private final AtomicReference<String> watchdogScriptHash = new AtomicReference<>(null);

    public Watchdog(Config config) {
        this.jedisPool = PoolUtils.createJedisPool(config, new WatchdogJedisPoolConfig());

        activate();
    }

    private void loadScript() {
        try {
            doWorkInPool(this.jedisPool, (PoolUtils.PoolWork<Jedis, Void>) jedis -> {
                watchdogScriptHash.set(jedis.scriptLoad(ScriptUtils.readScript(WATCHDOG_LUA)));
                return null;
            });
        } catch (Exception e) {
            LOG.error("Failed to load script " + WATCHDOG_LUA, e);
            throw new RuntimeException(e);
        }
    }

    private void activate() {
        final String isDebug = LOG.isDebugEnabled() ? Boolean.TRUE.toString() : Boolean.FALSE.toString();
        final String serverName = getServerName();

        loadScript();
        activateIsAliveService(serverName);
        activateWatchdogService(serverName, isDebug);
        requeueLostJobs(serverName, isDebug);
    }

    private void requeueLostJobs(final String serverName, final String isDebug) {
        // On watchdog activation check whether server contains unhandled in-flight jobs and re-queuing them

        int requeueJobsRetryNum = 0;


        while (requeueJobsRetryNum <= REQUEUE_JOBS_RETRY_NUM) {
            try {
                doWorkInPool(this.jedisPool, (PoolUtils.PoolWork<Jedis, Void>) jedis -> {
                    jedis.evalsha(watchdogScriptHash.get(), WATCHDOG_SCRIPT_KEYS_NUMBER, serverName, REQUEUE_JOBS, isDebug);
                    return null;
                });
                break;
            } catch (Exception e) {
                requeueJobsRetryNum++;
                LOG.error("Failed to run " + REQUEUE_JOBS + " job " + WATCHDOG_LUA + " script, retry " + requeueJobsRetryNum, e);
            }
        }
    }

    private void activateWatchdogService(final String serverName, final String isDebug) {
        // Schedule watchdog job , checking whether one of the servers was inactive too long and re-queuing jobs of a such inactive server
        final Runnable lightKeeper = () -> {

            try {
                doWorkInPool(this.jedisPool, (PoolUtils.PoolWork<Jedis, Void>) jedis -> {
                    jedis.evalsha(watchdogScriptHash.get(), WATCHDOG_SCRIPT_KEYS_NUMBER, serverName, WATCHDOG, isDebug, String.valueOf(TIME_TO_REQUEUE_JOBS_ON_INACTIVE_SERVER_SEC), String.valueOf(LIGHT_KEEPER_PERIOD));
                    return null;
                });
            } catch (Exception e) {
                LOG.error("Failed to run " + WATCHDOG + " job " + WATCHDOG_LUA + " script", e);
            }
        };

        scheduler.scheduleAtFixedRate(lightKeeper, LIGHT_KEEPER_PERIOD, LIGHT_KEEPER_PERIOD, SECONDS);
    }

    private void activateIsAliveService(final String serverName) throws RuntimeException {
        // Schedule isAlive job used to mark in redis server is still alive
        final Runnable isAlive = () -> {

            try {
                doWorkInPool(this.jedisPool, (PoolUtils.PoolWork<Jedis, Void>) jedis -> {
                    jedis.evalsha(watchdogScriptHash.get(), WATCHDOG_SCRIPT_KEYS_NUMBER, serverName, IS_ALIVE);
                    return null;
                });
            } catch (Exception e) {
                LOG.error("Failed to run " + IS_ALIVE + " function " + WATCHDOG_LUA + " script", e);
            }
        };

        scheduler.scheduleAtFixedRate(isAlive, INITIAL_DELAY, IS_ALIVE_PERIOD, SECONDS);
    }

    private String getServerName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException uhe) {
            throw new RuntimeException(uhe);
        }
    }

}


class WatchdogJedisPoolConfig extends JedisPoolConfig {
    @Override
    public int getMaxTotal() {
        return 2;
    }

    @Override
    public int getMaxIdle() {
        return 2;
    }

    @Override
    public int getMinIdle() {
        return 0;
    }
}