package net.greghaines.jesque.utils;

import static net.greghaines.jesque.utils.JesqueUtils.createBacktrace;
import static net.greghaines.jesque.utils.JesqueUtils.recreateThrowable;

import net.greghaines.jesque.JobFailure;
import net.greghaines.jesque.json.ObjectMapperFactory;
import org.junit.Assert;
import org.junit.Test;

public class TestExceptionSerialization {
    
    @Test
    public void simpleNoMsg() throws Exception {
        try {
            throw new Exception();
        } catch (Throwable t) {
            serialize(t);
        }
    }

    @Test
    public void simpleWithMsg() throws Exception {
        try {
            throw new Exception("FOO: tricky!");
        } catch (Throwable t) {
            serialize(t);
        }
    }

    @Test
    public void nestedOneDeepNoMsg() throws Exception {
        try {
            try {
                throw new Exception();
            } catch (Throwable t1) {
                throw new RuntimeException(t1);
            }
        } catch (Throwable t) {
            serialize(t);
        }
    }

    @Test
    public void nestedOneDeepWithDeepMsg() throws Exception {
        try {
            try {
                throw new Exception("FOO: tricky!");
            } catch (Throwable t1) {
                throw new RuntimeException(t1);
            }
        } catch (Throwable t) {
            serialize(t);
        }
    }

    @Test
    public void nestedOneDeepWithSurfaceMsg() throws Exception {
        try {
            try {
                throw new Exception();
            } catch (Throwable t1) {
                throw new RuntimeException("FOO: tricky!", t1);
            }
        } catch (Throwable t) {
            serialize(t);
        }
    }

    @Test
    public void nestedOneDeepWithBothMsg() throws Exception {
        try {
            try {
                throw new Exception("Thar' she blows...");
            } catch (Throwable t1) {
                throw new RuntimeException("FOO: tricky!", t1);
            }
        } catch (Throwable ex) {
            serialize(ex);
        }
    }

    @Test
    public void nestedSuperDeepWithCrazyMsg() throws Exception {
        try {
            try {
                try {
                    try {
                        throw new Exception("Thar' she blows...");
                    } catch (Throwable t3) {
                        throw new IllegalStateException(t3);
                    }
                } catch (Throwable t2) {
                    throw new IllegalArgumentException(t2);
                }
            } catch (Throwable t1) {
                throw new RuntimeException("FOO: tricky!", t1);
            }
        } catch (Throwable t) {
            serialize(t);
        }
    }

    @Test(expected = NoSuchConstructorException.class)
    public void simpleNonRegularException() throws Exception {
        try {
            throw new NonRegularException(0.0);
        } catch (Throwable t) {
            serialize(t);
        }
    }

    @Test(expected = NoSuchConstructorException.class)
    public void nestedNonRegularException() throws Exception {
        try {
            try {
                throw new NonRegularException(0.0);
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
        } catch (Throwable t2) {
            serialize(t2);
        }
    }

    @Test
    public void testDeseralize() throws Exception {
        final String payload = "{\"worker\":\"test5:12385-1:JAVA_DYNAMIC_QUEUES,SomeQueue\",\"queue\":\"audioTranscode\",\"payload\":{\"class\":\"SomeClass\",\"args\":[1234]},\"exception\":\"groovy.lang.MissingMethodException\",\"error\":\"SomeError\",\"backtrace\":[\"\\tat sompackage.someclass(somefile.java:42)\"],\"failed_at\":\"2011-10-17T18:01:33.185+0000\",\"retried_at\":null}";
        final JobFailure jobFailure = ObjectMapperFactory.get().readValue(payload, JobFailure.class);
        Assert.assertNull(jobFailure.getThrowable());
    }

    private static void serialize(final Throwable t) throws Exception {
        assertEquals(t, recreateThrowable(t.getClass().getName(), t.getMessage(), createBacktrace(t)));
    }

    private static void assertEquals(final Throwable t, final Throwable newT) {
        Assert.assertEquals((t == null), (newT == null));
        if (t != null) {
            Assert.assertEquals(t.getClass(), newT.getClass());
            Assert.assertEquals(t.getMessage(), newT.getMessage());
            Assert.assertEquals((t.getCause() == null), (newT.getCause() == null));
            Assert.assertEquals(createBacktrace(t), createBacktrace(newT));
            if (t.getCause() != null) {
                assertEquals(t.getCause(), newT.getCause());
            }
        }
    }

    public static final class NonRegularException extends Exception {
        
        private static final long serialVersionUID = -5336815460233912386L;

        private final double number;

        public NonRegularException(final double number) {
            super(Double.toString(number));
            this.number = number;
        }

        public double getNumber() {
            return this.number;
        }
    }
}
