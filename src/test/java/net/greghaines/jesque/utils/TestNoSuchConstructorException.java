package net.greghaines.jesque.utils;

import org.junit.Assert;
import org.junit.Test;

public class TestNoSuchConstructorException {

    @Test
    public void testStringConstructor() {
        final NoSuchConstructorException nsce = new NoSuchConstructorException("foo");
        Assert.assertNull(nsce.getType());
        Assert.assertNull(nsce.getArgs());
    }

    @Test
    public void testClassArgsConstructor() {
        final NoSuchConstructorException nsce = new NoSuchConstructorException(
                TestNoSuchConstructorException.class, "foo", "bar");
        Assert.assertEquals(TestNoSuchConstructorException.class, nsce.getType());
        Assert.assertArrayEquals(new String[]{"foo", "bar"}, nsce.getArgs());
    }
}
