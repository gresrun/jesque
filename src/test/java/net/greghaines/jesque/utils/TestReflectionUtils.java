package net.greghaines.jesque.utils;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.junit.Test;

/** Tests ReflectionUtils. */
public class TestReflectionUtils {

  @Test
  public void testForName_Null() throws ClassNotFoundException {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          ReflectionUtils.forName(null);
        });
  }

  @Test
  public void testForName_NotFound() throws ClassNotFoundException {
    assertThrows(
        ClassNotFoundException.class,
        () -> {
          ReflectionUtils.forName("123Bogus");
        });
  }

  @Test
  public void testForName_Primitive() throws ClassNotFoundException {
    assertThat(ReflectionUtils.forName("int")).isSameInstanceAs(int.class);
  }

  @Test
  public void testForName_PrimitiveArray() throws ClassNotFoundException {
    assertThat(ReflectionUtils.forName("[I")).isSameInstanceAs(int[].class);
  }

  @Test
  public void testForName_Common() throws ClassNotFoundException {
    assertThat(ReflectionUtils.forName(String.class.getName())).isSameInstanceAs(String.class);
  }

  @Test
  public void testForName_Standard() throws ClassNotFoundException {
    assertThat(ReflectionUtils.forName(Map.class.getName())).isSameInstanceAs(Map.class);
  }

  @Test
  public void testForName_Inner() throws ClassNotFoundException {
    assertThat(ReflectionUtils.forName(Entry.class.getName())).isSameInstanceAs(Entry.class);
  }

  @Test
  public void testForName_InnerDot() throws ClassNotFoundException {
    assertThat(ReflectionUtils.forName("java.util.Map.Entry")).isSameInstanceAs(Entry.class);
  }

  @Test
  public void testForName_Array() throws ClassNotFoundException {
    assertThat(ReflectionUtils.forName("[Ljava.util.Map;")).isSameInstanceAs(Map[].class);
  }

  @Test
  public void testForName_ArraySuffix() throws ClassNotFoundException {
    assertThat(ReflectionUtils.forName("java.lang.String[]")).isSameInstanceAs(String[].class);
  }

  @Test
  public void testForName_DoubleArray() throws ClassNotFoundException {
    assertThat(ReflectionUtils.forName("[[Ljava.lang.String;")).isSameInstanceAs(String[][].class);
  }

  @Test
  public void testForName_DoubleArraySuffix() throws ClassNotFoundException {
    assertThat(ReflectionUtils.forName("java.lang.String[][]")).isSameInstanceAs(String[][].class);
  }

  @Test
  public void testInvokeSetters_NullInstance() throws ReflectiveOperationException {
    assertThat((Object) ReflectionUtils.invokeSetters(null, null)).isNull();
  }

  @Test
  public void testInvokeSetters_NullVars() throws ReflectiveOperationException {
    final Object obj = new Object();
    assertThat(ReflectionUtils.invokeSetters(obj, null)).isSameInstanceAs(obj);
  }

  @Test
  public void testInvokeSetters_EmptyVars() throws ReflectiveOperationException {
    final Object obj = new Object();
    assertThat(ReflectionUtils.invokeSetters(obj, new HashMap<String, Object>()))
        .isSameInstanceAs(obj);
  }

  @Test
  public void testInvokeSetters_MissingSetter() throws ReflectiveOperationException {
    final Map<String, Object> vars = Map.of("bogusVal", new Object());
    assertThrows(
        NoSuchMethodException.class,
        () -> {
          ReflectionUtils.invokeSetters(new SetterObj(), vars);
        });
  }

  @Test
  public void testInvokeSetters_BadType() throws ReflectiveOperationException {
    final Map<String, Object> vars = Map.of("intVal", new Object());
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          ReflectionUtils.invokeSetters(new SetterObj(), vars);
        });
  }

  @Test
  public void testInvokeSetters_Success() throws ReflectiveOperationException {
    final Map<String, Object> vars =
        Map.of("intVal", 1, "floatVal", 2.3f, "stringVal", "foobar", "objVal", new Object());
    final SetterObj obj = ReflectionUtils.invokeSetters(new SetterObj(), vars);
    assertThat(obj).isNotNull();
    assertThat(obj.getIntVal()).isEqualTo(((Integer) vars.get("intVal")).intValue());
    assertThat(obj.getFloatVal()).isEqualTo(((Float) vars.get("floatVal")).floatValue());
    assertThat(obj.getStringVal()).isEqualTo((String) vars.get("stringVal"));
    assertThat(obj.getObjVal()).isEqualTo(vars.get("objVal"));
  }

  @Test
  public void testCreateObject_NoArgs_NoVars()
      throws NoSuchConstructorException,
          AmbiguousConstructorException,
          ReflectiveOperationException {
    final SetterObj obj = ReflectionUtils.createObject(SetterObj.class, null, null);
    assertThat(obj).isNotNull();
  }

  @Test
  public void testCreateObject_NoArgs_Vars()
      throws NoSuchConstructorException,
          AmbiguousConstructorException,
          ReflectiveOperationException {
    final Map<String, Object> vars =
        Map.of("intVal", 1, "floatVal", 2.3f, "stringVal", "foobar", "objVal", new Object());
    final SetterObj obj = ReflectionUtils.createObject(SetterObj.class, null, vars);
    assertThat(obj).isNotNull();
    assertThat(obj.getIntVal()).isEqualTo(((Integer) vars.get("intVal")).intValue());
    assertThat(obj.getFloatVal()).isEqualTo(((Float) vars.get("floatVal")).floatValue());
    assertThat(obj.getStringVal()).isEqualTo((String) vars.get("stringVal"));
    assertThat(obj.getObjVal()).isEqualTo(vars.get("objVal"));
  }

  @Test
  public void testCreateObject_Args_NoVars()
      throws NoSuchConstructorException,
          AmbiguousConstructorException,
          ReflectiveOperationException {
    final Object[] args = {1, 2.3f, "foobar", new Object()};
    final SetterObj obj = ReflectionUtils.createObject(SetterObj.class, args, null);
    assertThat(obj).isNotNull();
    assertThat(obj.getIntVal()).isEqualTo(((Integer) args[0]).intValue());
    assertThat(obj.getFloatVal()).isEqualTo(((Float) args[1]).floatValue());
    assertThat(obj.getStringVal()).isEqualTo((String) args[2]);
    assertThat(obj.getObjVal()).isEqualTo(args[3]);
  }

  @Test
  public void testCreateObject_EmptyArgs_Vars()
      throws NoSuchConstructorException,
          AmbiguousConstructorException,
          ReflectiveOperationException {
    final Map<String, Object> vars =
        Map.of("intVal", 1, "floatVal", 2.3f, "stringVal", "foobar", "objVal", new Object());
    final SetterObj obj = ReflectionUtils.createObject(SetterObj.class, new Object[0], vars);
    assertThat(obj).isNotNull();
    assertThat(obj.getIntVal()).isEqualTo(((Integer) vars.get("intVal")).intValue());
    assertThat(obj.getFloatVal()).isEqualTo(((Float) vars.get("floatVal")).floatValue());
    assertThat(obj.getStringVal()).isEqualTo((String) vars.get("stringVal"));
    assertThat(obj.getObjVal()).isEqualTo(vars.get("objVal"));
  }

  public static class SetterObj implements Runnable {

    private int intVal;
    private float floatVal;
    private String stringVal;
    private Object objVal;

    public SetterObj() {
      // Do nothing
    }

    public SetterObj(
        final int intVal, final float floatVal, final String stringVal, final Object objVal) {
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
