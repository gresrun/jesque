package net.greghaines.jesque;

import redis.clients.jedis.Jedis;

/**
 * Created by dimav
 * on 14/09/17 15:32.
 */
public class JobUniquenessValidator {
    private static final String UNIQUE_KEY_PREFIX = "resque:uniqueKey:";
    private final UniqueKeyExtractor uniqueKeyExtractor;

    public JobUniquenessValidator(final UniqueKeyExtractor uniqueKeyExtractor) {
        this.uniqueKeyExtractor = uniqueKeyExtractor;
    }

    public UniqueKeyExtractor getUniqueKeyExtractor() {
        return uniqueKeyExtractor;
    }

    public String generateRedisKeyFromUniqueJobKey(final String uniqueJobKey) {
        return UNIQUE_KEY_PREFIX + uniqueJobKey;
    }
}

