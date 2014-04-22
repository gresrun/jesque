package net.greghaines.jesque.utils;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests ReflectionUtils.
 */
public class TestReflectionUtils {

    @Test
    public void testInvokeSetters_NullInstance() throws ReflectiveOperationException {
        Assert.assertNull(ReflectionUtils.invokeSetters(null, null));
    }
    
    @Test
    public void testInvokeSetters_NullVars() throws ReflectiveOperationException {
        final Object obj = new Object();
        Assert.assertSame(obj, ReflectionUtils.invokeSetters(obj, null));
    }
    
    @Test
    public void testInvokeSetters_EmptyVars() throws ReflectiveOperationException {
        final Object obj = new Object();
        Assert.assertSame(obj, ReflectionUtils.invokeSetters(obj, new HashMap<String,Object>()));
    }
    
    @Test(expected = NoSuchMethodException.class)
    public void testInvokeSetters_MissingSetter() throws ReflectiveOperationException {
        final Map<String,Object> vars = new HashMap<String,Object>();
        vars.put("bogusVal", new Object());
        ReflectionUtils.invokeSetters(new SetterObj(), vars);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testInvokeSetters_BadType() throws ReflectiveOperationException {
        final Map<String,Object> vars = new HashMap<String,Object>();
        vars.put("intVal", new Object());
        ReflectionUtils.invokeSetters(new SetterObj(), vars);
    }
    
    @Test
    public void testInvokeSetters_Success() throws ReflectiveOperationException {
        final Map<String,Object> vars = new HashMap<String,Object>();
        vars.put("intVal", 1);
        vars.put("floatVal", 2.3f);
        vars.put("stringVal", "foobar");
        vars.put("objVal", new Object());
        final SetterObj obj = ReflectionUtils.invokeSetters(new SetterObj(), vars);
        Assert.assertNotNull(obj);
        Assert.assertEquals(vars.get("intVal"), obj.getIntVal());
        Assert.assertEquals(vars.get("floatVal"), obj.getFloatVal());
        Assert.assertEquals(vars.get("stringVal"), obj.getStringVal());
        Assert.assertEquals(vars.get("objVal"), obj.getObjVal());
    }
    
    public static class SetterObj {
        
        private int intVal;
        private float floatVal;
        private String stringVal;
        private Object objVal;
        
        public int getIntVal() {
            return this.intVal;
        }

        public float getFloatVal() {
            return this.floatVal;
        }

        public String getStringVal() {
            return this.stringVal;
        }

        public Object getObjVal() {
            return this.objVal;
        }

        public void setIntVal(final int intVal) {
            this.intVal = intVal;
        }
        
        public void setFloatVal(final float floatVal) {
            this.floatVal = floatVal;
        }
        
        public void setStringVal(final String stringVal) {
            this.stringVal = stringVal;
        }
        
        public void setObjVal(final Object objVal) {
            this.objVal = objVal;
        }
    }
}
