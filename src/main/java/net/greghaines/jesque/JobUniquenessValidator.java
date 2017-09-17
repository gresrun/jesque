package net.greghaines.jesque;

import redis.clients.jedis.Jedis;

/**
 * Created by dimav
 * on 14/09/17 15:32.
 */
public class JobUniquenessValidator {
    private static final String UNIQUE_KEY_PREFIX = "resque:uniqueKey:";
    private static final int UNIQUE_KEY_EXPIRATION_PERIOD = 30;
    private final UniqueKeyExtractor uniqueKeyExtractor;

    public JobUniquenessValidator(final UniqueKeyExtractor uniqueKeyExtractor) {
        this.uniqueKeyExtractor = uniqueKeyExtractor;
    }

    public UniqueKeyExtractor getUniqueKeyExtractor() {
        return uniqueKeyExtractor;
    }

    public boolean checkJobUniqueness(Jedis jedis, String jobJson) {
        final String uniqueJobKey = uniqueKeyExtractor.getUniqueKeyFromJob(jobJson);
        final String uniqueKey = generateRedisKeyFromUniqueJobKey(uniqueJobKey);
        return registerKeyIfNotExists(jedis, uniqueKey);
    }

    public String generateRedisKeyFromUniqueJobKey(final String uniqueJobKey) {
        return UNIQUE_KEY_PREFIX + uniqueJobKey;
    }

    private boolean registerKeyIfNotExists(final Jedis jedis, final String uniqueKey) {
        long ret = jedis.setnx(uniqueKey, "on");
        if (ret != 0) {
            jedis.expire(uniqueKey, UNIQUE_KEY_EXPIRATION_PERIOD);
        }
        return ret == 0;
    }

 }

