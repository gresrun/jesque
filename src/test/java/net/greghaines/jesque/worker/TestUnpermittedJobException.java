package net.greghaines.jesque.worker;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests UnpermittedJobException.
 * 
 * @author Greg Haines
 */
public class TestUnpermittedJobException {

    @Test
    public void testGetMessage() {
        final String message = "Foo";
        final UnpermittedJobException upje = new UnpermittedJobException(message);
        Assert.assertEquals(message, upje.getMessage());
    }

    @Test
    public void testGetType() {
        final Class<?> type = String.class;
        final UnpermittedJobException upje = new UnpermittedJobException(type);
        Assert.assertEquals(type, upje.getType());
    }
}
