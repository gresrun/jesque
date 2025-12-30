package net.greghaines.jesque.utils;

import static com.google.common.truth.Truth.assertThat;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.worker.UnpermittedJobException;
import org.junit.Test;

public class TestJesqueUtils {

  @Test
  public void testJoin_VarArgs() {
    assertThat(JesqueUtils.join(",", "foo", "bar", "baz")).isEqualTo("foo,bar,baz");
  }

  @Test
  public void testJoin_Iterable() {
    assertThat(JesqueUtils.join(",", Arrays.asList("foo", "bar", "baz"))).isEqualTo("foo,bar,baz");
  }

  @Test
  public void testCreateKey_VarArgs() {
    assertThat(JesqueUtils.createKey("", "bar", "baz")).isEqualTo("bar:baz");
  }

  @Test
  public void testCreateKey_Iterable() {
    assertThat(JesqueUtils.createKey("foo", Arrays.asList("bar", "baz"))).isEqualTo("foo:bar:baz");
  }

  @Test
  public void testMaterializeJob() throws Exception {
    final Object action = JesqueUtils.materializeJob(new Job(TestRunnableJob.class.getName()));
    assertThat(action).isInstanceOf(TestRunnableJob.class);
    final Object action2 = JesqueUtils.materializeJob(new Job(TestCallableJob.class.getName()));
    assertThat(action2).isInstanceOf(TestCallableJob.class);
  }

  @Test(expected = ClassCastException.class)
  public void testMaterializeJob_NotRunnable() throws Exception {
    JesqueUtils.materializeJob(new Job(TestBadJob.class.getName()));
  }

  @Test
  public void testMaterializeJob_Types() throws Exception {
    final Map<String, Class<?>> jobTypes =
        Map.of("TestRunnableJob", TestRunnableJob.class, "TestCallableJob", TestCallableJob.class);
    final Object action = JesqueUtils.materializeJob(new Job("TestRunnableJob"), jobTypes);
    assertThat(action).isInstanceOf(TestRunnableJob.class);
    final Object action2 = JesqueUtils.materializeJob(new Job("TestCallableJob"), jobTypes);
    assertThat(action2).isInstanceOf(TestCallableJob.class);
  }

  @Test(expected = UnpermittedJobException.class)
  public void testMaterializeJob_Types_NotPermitted() throws Exception {
    final Map<String, Class<?>> jobTypes = Map.of();
    JesqueUtils.materializeJob(new Job("TestRunnableJob"), jobTypes);
  }

  @Test(expected = ClassCastException.class)
  public void testMaterializeJob_Types_NotRunnable() throws Exception {
    final Map<String, Class<?>> jobTypes = Map.of("TestBadJob", TestBadJob.class);
    JesqueUtils.materializeJob(new Job("TestBadJob"), jobTypes);
  }

  @Test
  public void testRecreateStackTrace() throws ParseException {
    final List<String> bTrace =
        new ArrayList<String>(
            Arrays.asList(
                "foo",
                "\tat net.greghaines.jesque.Job.<init>(Job.java:30)",
                "\tat net.greghaines.jesque.Job.<init>(Job.java)",
                "\tat net.greghaines.jesque.Job.<init>(Unknown Source)",
                "\tat net.greghaines.jesque.Job.<init>(Native Method)"));
    final StackTraceElement[] stes = JesqueUtils.recreateStackTrace(bTrace);
    assertThat(stes).hasLength(4);
  }

  @Test(expected = ParseException.class)
  public void testRecreateStackTrace_BadFormat() throws ParseException {
    final List<String> bTrace = new ArrayList<String>(Arrays.asList("\tat net.greghaines"));
    final StackTraceElement[] stes = JesqueUtils.recreateStackTrace(bTrace);
    assertThat(stes).isEmpty();
  }

  @Test
  public void testRecreateStackTrace_NotFormat() throws ParseException {
    final List<String> bTrace = Arrays.asList("foo", "bar");
    final StackTraceElement[] stes = JesqueUtils.recreateStackTrace(bTrace);
    assertThat(stes).isEmpty();
  }

  @Test
  public void testRecreateStackTrace_Empty() throws ParseException {
    final StackTraceElement[] stes = JesqueUtils.recreateStackTrace(new ArrayList<String>());
    assertThat(stes).isEmpty();
  }

  @Test
  public void testRecreateStackTrace_Null() throws ParseException {
    final StackTraceElement[] stes = JesqueUtils.recreateStackTrace(null);
    assertThat(stes).isEmpty();
  }

  @Test
  public void testEqual() {
    Throwable ex1 = new RuntimeException();
    assertThat(JesqueUtils.equal(ex1, ex1)).isTrue();
    assertThat(JesqueUtils.equal(ex1, null)).isFalse();
    assertThat(JesqueUtils.equal(null, ex1)).isFalse();
    Throwable ex2 = new Exception();
    assertThat(JesqueUtils.equal(ex1, ex2)).isFalse();
    ex2 = new RuntimeException();
    assertThat(JesqueUtils.equal(ex1, ex2)).isTrue();
    ex2 = new RuntimeException("foo");
    assertThat(JesqueUtils.equal(ex1, ex2)).isFalse();
    ex1 = new RuntimeException("bar");
    assertThat(JesqueUtils.equal(ex1, ex2)).isFalse();
    ex1 = new RuntimeException("foo");
    assertThat(JesqueUtils.equal(ex1, ex2)).isTrue();
    ex1 = new RuntimeException("foo", new Exception());
    assertThat(JesqueUtils.equal(ex1, ex2)).isFalse();
    ex2 = new RuntimeException("foo", new Exception());
    assertThat(JesqueUtils.equal(ex1, ex2)).isTrue();
  }

  public static class TestRunnableJob implements Runnable {
    @Override
    public void run() {
      // Do nothing
    }
  }

  public static class TestCallableJob implements Callable<Object> {
    @Override
    public Object call() {
      // Do nothing
      return null;
    }
  }

  public static class TestBadJob {
    public void run() {
      // Do nothing
    }
  }
}
