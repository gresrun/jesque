package net.greghaines.jesque.meta;

import java.util.Locale;

import org.junit.Assert;
import org.junit.Test;

public class TestKeyType {

    @Test
    public void testToString() {
        for (final KeyType keyType : KeyType.values()) {
            Assert.assertEquals(keyType.name().toLowerCase(Locale.US), keyType.toString());
        }
    }

    @Test
    public void testGetKeyTypeByValue() {
        for (final KeyType keyType : KeyType.values()) {
            Assert.assertEquals(keyType, KeyType.getKeyTypeByValue(keyType.name().toLowerCase(Locale.US)));
        }
    }
}
