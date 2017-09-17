package net.greghaines.jesque;

/**
 * Created by dimav
 * on 14/09/17 15:16.
 */
public interface UniqueKeyExtractor {
    String getUniqueKeyFromJob(String jobJson);
}
