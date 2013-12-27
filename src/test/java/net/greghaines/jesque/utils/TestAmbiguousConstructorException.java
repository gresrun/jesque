package net.greghaines.jesque.utils;

import java.lang.reflect.Constructor;
import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

public class TestAmbiguousConstructorException {

    @Test
    public void testStringConstructor() {
        final AmbiguousConstructorException ace = new AmbiguousConstructorException("foo");
        Assert.assertNull(ace.getType());
        Assert.assertNull(ace.getArgs());
    }

    @Test
    public void testClassArgsOptionsConstructor() {
        final Set<Constructor<?>> options = new HashSet<Constructor<?>>();
        final AmbiguousConstructorException ace = new AmbiguousConstructorException(
                TestAmbiguousConstructorException.class, new Object[]{"foo", "bar"}, options);
        Assert.assertEquals(TestAmbiguousConstructorException.class, ace.getType());
        Assert.assertArrayEquals(new Object[]{"foo", "bar"}, ace.getArgs());
        Assert.assertEquals(options, ace.getOptions());
    }
}
