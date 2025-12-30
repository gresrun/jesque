package net.greghaines.jesque;

import static com.google.common.truth.Truth.assertThat;

import java.util.Arrays;
import java.util.Map;
import org.junit.Test;

public class TestJob {

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_VarArgs_NullName() {
    new Job(null, 1, 2.0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_VarArgs_EmptyName() {
    new Job("", 1, 2.0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_NamedArgs_NullName() {
    new Job(null, Map.of("foo", "bar"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_NamedArgs_EmptyName() {
    new Job("", Map.of("foo", "bar"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_AllArgs_NullName() {
    new Job(null, new Object[] {true, 1}, Map.of("foo", "bar"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_AllArgs_EmptyName() {
    new Job("", new Object[] {true, 1}, Map.of("foo", "bar"));
  }

  @Test
  public void testConstructor_NoArg() {
    final Job job = new Job();
    assertThat(job.getClassName()).isNull();
    assertThat(job.getArgs()).isNull();
    assertThat(job.getVars()).isNull();
    assertThat(job.isValid()).isFalse();
    assertThat(job.toString()).isNotNull();
  }

  @Test
  public void testConstructor_VarArgs() {
    final String className = "foo";
    final Object arg1 = 1;
    final Object arg2 = 2.0;
    final Job job = new Job(className, arg1, arg2);
    assertThat(job.getClassName()).isEqualTo(className);
    assertThat(job.getVars()).isNull();
    final Object[] args = job.getArgs();
    assertThat(args).isNotNull();
    assertThat(args).isEqualTo(new Object[] {arg1, arg2});
    assertThat(job.isValid()).isTrue();
    assertThat(job.toString()).isNotNull();
  }

  @Test
  public void testConstructor_ListArgs() {
    final String className = "foo";
    final Object arg1 = 1;
    final Object arg2 = 2.0;
    final Job job = new Job(className, Arrays.asList(arg1, arg2));
    assertThat(job.getClassName()).isEqualTo(className);
    assertThat(job.getVars()).isNull();
    final Object[] args = job.getArgs();
    assertThat(args).isNotNull();
    assertThat(args).isEqualTo(new Object[] {arg1, arg2});
    assertThat(job.isValid()).isTrue();
    assertThat(job.toString()).isNotNull();
  }

  @Test
  public void testConstructor_NamedArgs() {
    final String className = "foo";
    final Map<String, ? extends Object> vars = Map.of("foo", "bar", "baz", 123);
    final Job job = new Job(className, vars);
    assertThat(job.getClassName()).isEqualTo(className);
    assertThat(job.getArgs()).isNull();
    assertThat(job.getVars()).isEqualTo(vars);
    assertThat(job.isValid()).isTrue();
    assertThat(job.toString()).isNotNull();
  }

  @Test
  public void testConstructor_AllArgs() {
    final String className = "foo";
    final Object[] args = {1, 2.0};
    final Map<String, ? extends Object> vars = Map.of("foo", "bar", "baz", 123);
    final Job job = new Job(className, args, vars);
    assertThat(job.getClassName()).isEqualTo(className);
    assertThat(job.getArgs()).isNotNull();
    assertThat(job.getArgs()).isEqualTo(args);
    assertThat(job.getVars()).isEqualTo(vars);
    assertThat(job.isValid()).isTrue();
    assertThat(job.toString()).isNotNull();
  }

  @Test
  public void testConstructor_Clone() {
    final String className = "foo";
    final Object[] args = {1, 2.0};
    final Map<String, ? extends Object> vars = Map.of("foo", "bar", "baz", 123);
    final Job protoJob = new Job(className, args, vars);
    final Job job = new Job(protoJob);
    assertThat(job.getClassName()).isEqualTo(className);
    final Object[] newArgs = job.getArgs();
    assertThat(newArgs).isNotNull();
    assertThat(newArgs).isEqualTo(args);
    assertThat(job.getVars()).isEqualTo(vars);
    assertThat(job.isValid()).isEqualTo(protoJob.isValid());
    TestUtils.assertFullyEquals(protoJob, job);
    final Job protoJob2 = new Job(className, null, null);
    final Job job2 = new Job(protoJob2);
    assertThat(job2.getClassName()).isEqualTo(className);
    assertThat(job2.getArgs()).isNull();
    assertThat(job2.getVars()).isNull();
    assertThat(job2.isValid()).isEqualTo(protoJob2.isValid());
    TestUtils.assertFullyEquals(protoJob2, job2);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_Clone_Null() {
    new Job((Job) null);
  }

  @Test
  public void testSetters() {
    final String className1 = "foo";
    final String className2 = "";
    final Object arg1 = 1;
    final Object arg2 = 2.0;
    final Map<String, ? extends Object> vars = Map.of("foo", "bar", "baz", 123);
    final Job job = new Job();
    assertThat(job.getClassName()).isNull();
    assertThat(job.getArgs()).isNull();
    assertThat(job.getVars()).isNull();
    assertThat(job.isValid()).isFalse();
    job.setArgs(arg1, arg2);
    assertThat(job.getArgs()).isEqualTo(new Object[] {arg1, arg2});
    assertThat(job.isValid()).isFalse();
    job.setVars(vars);
    assertThat(job.getVars()).isEqualTo(vars);
    assertThat(job.isValid()).isFalse();
    job.setClassName(className1);
    assertThat(job.getClassName()).isEqualTo(className1);
    assertThat(job.isValid()).isTrue();
    job.setClassName(className2);
    assertThat(job.getClassName()).isEqualTo(className2);
    assertThat(job.isValid()).isFalse();
  }

  @Test
  public void testEquals() {
    final Job job1 = new Job();
    TestUtils.assertFullyEquals(job1, job1);
    assertThat(job1.equals(null)).isFalse();
    assertThat(job1).isNotEqualTo(new Object());
    final Job job2 = new Job();
    TestUtils.assertFullyEquals(job1, job2);
    job2.setClassName("foo");
    assertThat(job1).isNotEqualTo(job2);
    job1.setClassName("bar");
    assertThat(job1).isNotEqualTo(job2);
    job1.setClassName("foo");
    TestUtils.assertFullyEquals(job1, job2);
    final Object arg1 = 1;
    final Object arg2 = 2.0;
    job2.setArgs(arg1, arg2);
    assertThat(job1).isNotEqualTo(job2);
    job1.setArgs(arg1, arg2);
    TestUtils.assertFullyEquals(job1, job2);
    final Map<String, ? extends Object> vars = Map.of("foo", "bar", "baz", 123);
    job2.setVars(vars);
    assertThat(job1).isNotEqualTo(job2);
    job1.setVars(vars);
    TestUtils.assertFullyEquals(job1, job2);
  }
}
