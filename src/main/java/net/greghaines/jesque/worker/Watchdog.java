package net.greghaines.jesque.worker;

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

    private final Jedis jedis;
    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(POOL_SIZE);
    private final AtomicReference<String> watchdogScriptHash = new AtomicReference<>(null);


    public Watchdog(Jedis jedis) {
        this.jedis = jedis;

        try {
            this.watchdogScriptHash
                    .set(jedis.scriptLoad(ScriptUtils.readScript(WATCHDOG_LUA)));
        } catch (IOException e) {
            LOG.error("Failed to load script " + WATCHDOG_LUA, e);
            throw new RuntimeException(e);
        }

        activate();
    }

    private void activate() {
        String isDebug = LOG.isDebugEnabled() ? Boolean.TRUE.toString() : Boolean.FALSE.toString();
        final String serverName = getServerName();

        // Schedule isAlive job used to mark in redis server is still alive
        final Runnable isAlive = () -> {
            try {
                jedis.evalsha(this.watchdogScriptHash.get(), WATCHDOG_SCRIPT_KEYS_NUMBER, serverName, IS_ALIVE);
            } catch (RuntimeException e) {
                LOG.error("Failed to run " + IS_ALIVE + " function " + WATCHDOG_LUA + " script", e);
            }
        };

        scheduler.scheduleAtFixedRate(isAlive, INITIAL_DELAY, IS_ALIVE_PERIOD, SECONDS);

        // Schedule watchdog job , checking whether one of the servers was inactive too long and re-queuing jobs of a such inactive server
        final Runnable lightKeeper = () -> {
            try {
                jedis.evalsha(this.watchdogScriptHash.get(), WATCHDOG_SCRIPT_KEYS_NUMBER, serverName, WATCHDOG, isDebug, String.valueOf(TIME_TO_REQUEUE_JOBS_ON_INACTIVE_SERVER_SEC), String.valueOf(LIGHT_KEEPER_PERIOD));
            } catch (RuntimeException e) {
                LOG.error("Failed to run " + WATCHDOG + " job " + WATCHDOG_LUA + " script", e);
            }
        };

        scheduler.scheduleAtFixedRate(lightKeeper, LIGHT_KEEPER_PERIOD, LIGHT_KEEPER_PERIOD, SECONDS);

        // On watchdog activation check whether server contains unhandled in-flight jobs and re-queuing them
        try {
            jedis.evalsha(this.watchdogScriptHash.get(), WATCHDOG_SCRIPT_KEYS_NUMBER, serverName, REQUEUE_JOBS, isDebug);
        } catch (RuntimeException e) {
            LOG.error("Failed to run " + REQUEUE_JOBS + " job " + WATCHDOG_LUA + " script", e);
        }


    }

    private String getServerName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException uhe) {
            throw new RuntimeException(uhe);
        }
    }
}

