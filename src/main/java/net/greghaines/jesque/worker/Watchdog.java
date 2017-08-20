package net.greghaines.jesque.worker;

import net.greghaines.jesque.Config;
import net.greghaines.jesque.utils.ScriptUtils;
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
    private static final int POOL_SIZE = 1;
    private static final int INITIAL_DELAY = 0;
    private static final int LIGHTHOUSE_PERIOD = 2;
    private static final String LIGHT_KEEPER_LUA = "/workerScripts/light_keeper.lua";
    private static final String REQUEUE_JOBS_LUA = "/workerScripts/requeue_jobs.lua";
    protected final Config config;
    protected final Jedis jedis;
    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(POOL_SIZE);
    private final AtomicReference<String> LightKeeperScriptHash = new AtomicReference<>(null);
    private final AtomicReference<String> RequeueJobsScriptHash = new AtomicReference<>(null);


    Watchdog(Config config, Jedis jedis) {
        this.config = config;
        this.jedis = jedis;

        try {
            this.LightKeeperScriptHash
                    .set(jedis.scriptLoad(ScriptUtils.readScript(LIGHT_KEEPER_LUA)));
            this.RequeueJobsScriptHash
                    .set(jedis.scriptLoad(ScriptUtils.readScript(REQUEUE_JOBS_LUA)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void activateLightKeeper() {
        jedis.evalsha(this.RequeueJobsScriptHash.get(), 1, getServerName());

        final Runnable lightkeeper = () -> {
            try {
                jedis.evalsha(this.LightKeeperScriptHash.get(), 1, getServerName());
            } catch (RuntimeException e) {
                e.printStackTrace();
            }
        };

        scheduler.scheduleAtFixedRate(lightkeeper, INITIAL_DELAY, LIGHTHOUSE_PERIOD, SECONDS);

    }

    private String getServerName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException uhe) {
            throw new RuntimeException(uhe);
        }
    }
}

