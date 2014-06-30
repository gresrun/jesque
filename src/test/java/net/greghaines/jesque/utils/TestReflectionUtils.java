package net.greghaines.jesque.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests ReflectionUtils.
 */
public class TestReflectionUtils {
    
    @Test(expected=IllegalArgumentException.class)
    public void testForName_Null() throws ClassNotFoundException {
        ReflectionUtils.forName(null);
    }
    
    @Test(expected=ClassNotFoundException.class)
    public void testForName_NotFound() throws ClassNotFoundException {
        ReflectionUtils.forName("123Bogus");
    }
    
    @Test
    public void testForName_Primitive() throws ClassNotFoundException {
        Assert.assertSame(int.class, ReflectionUtils.forName("int"));
    }
    
    @Test
    public void testForName_PrimitiveArray() throws ClassNotFoundException {
        Assert.assertSame(int[].class, ReflectionUtils.forName("[I"));
    }
    
    @Test
    public void testForName_Common() throws ClassNotFoundException {
        Assert.assertSame(String.class, ReflectionUtils.forName(String.class.getName()));
    }
    
    @Test
    public void testForName_Standard() throws ClassNotFoundException {
        Assert.assertSame(Map.class, ReflectionUtils.forName(Map.class.getName()));
    }
    
    @Test
    public void testForName_Inner() throws ClassNotFoundException {
        Assert.assertSame(Entry.class, ReflectionUtils.forName(Entry.class.getName()));
    }
    
    @Test
    public void testForName_InnerDot() throws ClassNotFoundException {
        Assert.assertSame(Entry.class, ReflectionUtils.forName("java.util.Map.Entry"));
    }
    
    @Test
    public void testForName_Array() throws ClassNotFoundException {
        Assert.assertSame(Map[].class, ReflectionUtils.forName("[Ljava.util.Map;"));
    }
    
    @Test
    public void testForName_ArraySuffix() throws ClassNotFoundException {
        Assert.assertSame(String[].class, ReflectionUtils.forName("java.lang.String[]"));
    }
    
    @Test
    public void testForName_DoubleArray() throws ClassNotFoundException {
        Assert.assertSame(String[][].class, ReflectionUtils.forName("[[Ljava.lang.String;"));
    }
    
    @Test
    public void testForName_DoubleArraySuffix() throws ClassNotFoundException {
        Assert.assertSame(String[][].class, ReflectionUtils.forName("java.lang.String[][]"));
    }

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
    
    @Test
    public void testCreateObject_NoArgs_NoVars() throws NoSuchConstructorException, AmbiguousConstructorException, 
            ReflectiveOperationException {
        final SetterObj obj = ReflectionUtils.createObject(SetterObj.class, null, null);
        Assert.assertNotNull(obj);
    }
    
    @Test
    public void testCreateObject_NoArgs_Vars() throws NoSuchConstructorException, AmbiguousConstructorException, 
            ReflectiveOperationException {
        final Map<String,Object> vars = new HashMap<String,Object>();
        vars.put("intVal", 1);
        vars.put("floatVal", 2.3f);
        vars.put("stringVal", "foobar");
        vars.put("objVal", new Object());
        final SetterObj obj = ReflectionUtils.createObject(SetterObj.class, null, vars);
        Assert.assertNotNull(obj);
        Assert.assertEquals(vars.get("intVal"), obj.getIntVal());
        Assert.assertEquals(vars.get("floatVal"), obj.getFloatVal());
        Assert.assertEquals(vars.get("stringVal"), obj.getStringVal());
        Assert.assertEquals(vars.get("objVal"), obj.getObjVal());
    }
    
    @Test
    public void testCreateObject_Args_NoVars() throws NoSuchConstructorException, AmbiguousConstructorException, 
            ReflectiveOperationException {
        final Object[] args = {1, 2.3f, "foobar", new Object()};
        final SetterObj obj = ReflectionUtils.createObject(SetterObj.class, args, null);
        Assert.assertNotNull(obj);
        Assert.assertEquals(args[0], obj.getIntVal());
        Assert.assertEquals(args[1], obj.getFloatVal());
        Assert.assertEquals(args[2], obj.getStringVal());
        Assert.assertEquals(args[3], obj.getObjVal());
    }
    
    @Test
    public void testCreateObject_EmptyArgs_Vars() throws NoSuchConstructorException, AmbiguousConstructorException, 
            ReflectiveOperationException {
        final Map<String,Object> vars = new HashMap<String,Object>();
        vars.put("intVal", 1);
        vars.put("floatVal", 2.3f);
        vars.put("stringVal", "foobar");
        vars.put("objVal", new Object());
        final SetterObj obj = ReflectionUtils.createObject(SetterObj.class, new Object[0], vars);
        Assert.assertNotNull(obj);
        Assert.assertEquals(vars.get("intVal"), obj.getIntVal());
        Assert.assertEquals(vars.get("floatVal"), obj.getFloatVal());
        Assert.assertEquals(vars.get("stringVal"), obj.getStringVal());
        Assert.assertEquals(vars.get("objVal"), obj.getObjVal());
    }
    
    public static class SetterObj implements Runnable {
        
        private int intVal;
        private float floatVal;
        private String stringVal;
        private Object objVal;
        
        public SetterObj() {
            // Do nothing
        }
        
        public SetterObj(final int intVal, final float floatVal, final String stringVal, final Object objVal) {
            this.intVal = intVal;
            this.floatVal = floatVal;
            this.stringVal = stringVal;
            this.objVal = objVal;
        }

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

        @Override
        public void run() {
            // Do nothing
        }
    }
}
