package net.greghaines.jesque.worker;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.utils.JedisUtils;
import net.greghaines.jesque.utils.ScriptUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Created by dimav on 17/08/17.
 * <p>
 * service responsible to requeue lost jobs
 */
public class Watchdog {
    private static final int RECONNECT_ATTEMPTS = 10000;
    private static final long RECONNECT_SLEEP_TIME = 1000;
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
    private static final String RECONNECTED_TO_REDIS = "Reconnected to Redis";
    private static final String CAN_T_RECONNECT_TO_REDIS = "Can't reconnect to Redis";
    private static final String RECONNECTING_TO_REDIS_IN_RESPONSE_TO_EXCEPTION = "Reconnecting to Redis in response to exception";
    private final Jedis jedis;
    private final Config config;
    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(POOL_SIZE);
    private final AtomicReference<String> watchdogScriptHash = new AtomicReference<>(null);

    public Watchdog(Config config, Jedis jedis) {
        this.config = config;
        this.jedis = jedis;

        activate();
    }

    private void loadScript() {
        try {
            this.watchdogScriptHash
                    .set(jedis.scriptLoad(ScriptUtils.readScript(WATCHDOG_LUA)));
        } catch (IOException e) {
            LOG.error("Failed to load script " + WATCHDOG_LUA, e);
            recoverFromException(e);
            loadScript();
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
        try {
            jedis.evalsha(this.watchdogScriptHash.get(), WATCHDOG_SCRIPT_KEYS_NUMBER, serverName, REQUEUE_JOBS, isDebug);
        } catch (RuntimeException e) {
            LOG.error("Failed to run " + REQUEUE_JOBS + " job " + WATCHDOG_LUA + " script", e);
            recoverFromException(e);
            requeueLostJobs(serverName, isDebug);
        }
    }

    private void activateWatchdogService(final String serverName, final String isDebug) {
        // Schedule watchdog job , checking whether one of the servers was inactive too long and re-queuing jobs of a such inactive server
        final Runnable lightKeeper = () -> {
            try {
                jedis.evalsha(this.watchdogScriptHash.get(), WATCHDOG_SCRIPT_KEYS_NUMBER, serverName, WATCHDOG, isDebug, String.valueOf(TIME_TO_REQUEUE_JOBS_ON_INACTIVE_SERVER_SEC), String.valueOf(LIGHT_KEEPER_PERIOD));
            } catch (RuntimeException e) {
                LOG.error("Failed to run " + WATCHDOG + " job " + WATCHDOG_LUA + " script", e);
                recoverFromException(e);
            }
        };

        scheduler.scheduleAtFixedRate(lightKeeper, LIGHT_KEEPER_PERIOD, LIGHT_KEEPER_PERIOD, SECONDS);
    }

    private void activateIsAliveService(final String serverName) {
        // Schedule isAlive job used to mark in redis server is still alive
        final Runnable isAlive = () -> {
            try {
                jedis.evalsha(this.watchdogScriptHash.get(), WATCHDOG_SCRIPT_KEYS_NUMBER, serverName, IS_ALIVE);
            } catch (RuntimeException e) {
                LOG.error("Failed to run " + IS_ALIVE + " function " + WATCHDOG_LUA + " script", e);
                recoverFromException(e);
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

    private void recoverFromException(final Exception ex) {
        LOG.info(RECONNECTING_TO_REDIS_IN_RESPONSE_TO_EXCEPTION, ex);
        while (!JedisUtils.reconnect(this.jedis, RECONNECT_ATTEMPTS, RECONNECT_SLEEP_TIME)) {
            LOG.warn(CAN_T_RECONNECT_TO_REDIS, ex);
        }

        authenticateAndSelectDB();
        LOG.info(RECONNECTED_TO_REDIS);
    }

    private void authenticateAndSelectDB() {
        if (this.config.getPassword() != null) {
            this.jedis.auth(this.config.getPassword());
        }
        this.jedis.select(this.config.getDatabase());
    }


}