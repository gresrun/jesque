package net.greghaines.jesque.worker;

import static com.google.common.truth.Truth.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import net.greghaines.jesque.Job;
import org.junit.Test;

/** Tests MapBasedJobFactory. */
public class TestMapBasedJobFactory {

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_Null() {
    new MapBasedJobFactory(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_NullName() throws Exception {
    final Map<String, Class<?>> jobTypes = new HashMap<String, Class<?>>();
    jobTypes.put(null, TestCallableJob.class);
    new MapBasedJobFactory(jobTypes);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_NullType() throws Exception {
    final Map<String, Class<?>> jobTypes = new HashMap<String, Class<?>>();
    jobTypes.put("TestCallableJob", null);
    new MapBasedJobFactory(jobTypes);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_NotRunnable() throws Exception {
    final Map<String, Class<?>> jobTypes = Map.of("TestBadJob", TestBadJob.class);
    new MapBasedJobFactory(jobTypes);
  }

  @Test
  public void testGetJobTypes() {
    final Map<String, Class<?>> jobTypes =
        Map.of("TestRunnableJob", TestRunnableJob.class, "TestCallableJob", TestCallableJob.class);
    final MapBasedJobFactory jobFactory = new MapBasedJobFactory(jobTypes);
    assertThat(jobFactory.getJobTypes()).isEqualTo(jobTypes);
  }

  @Test
  public void testsSetJobTypes() {
    final Map<String, Class<?>> jobTypes =
        Map.of("TestRunnableJob", TestRunnableJob.class, "TestCallableJob", TestCallableJob.class);
    final MapBasedJobFactory jobFactory = new MapBasedJobFactory(Map.of());
    assertThat(jobFactory.getJobTypes()).isEmpty();
    jobFactory.setJobTypes(jobTypes);
    assertThat(jobFactory.getJobTypes()).isEqualTo(jobTypes);
  }

  @Test
  public void testAddJobType() {
    final Map<String, Class<?>> jobTypes = new HashMap<String, Class<?>>();
    jobTypes.put("TestRunnableJob", TestRunnableJob.class);
    final MapBasedJobFactory jobFactory = new MapBasedJobFactory(jobTypes);
    assertThat(jobFactory.getJobTypes()).isEqualTo(jobTypes);
    jobFactory.addJobType("TestCallableJob", TestCallableJob.class);
    jobTypes.put("TestCallableJob", TestCallableJob.class);
    assertThat(jobFactory.getJobTypes()).isEqualTo(jobTypes);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAddJobType_NullName() {
    final MapBasedJobFactory jobFactory = new MapBasedJobFactory(Map.of());
    jobFactory.addJobType(null, TestCallableJob.class);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAddJobType_NullType() {
    final MapBasedJobFactory jobFactory = new MapBasedJobFactory(Map.of());
    jobFactory.addJobType("TestCallableJob", null);
  }

  @Test
  public void testRemoveJobName() {
    final Map<String, Class<?>> jobTypes = new HashMap<String, Class<?>>();
    jobTypes.put("TestRunnableJob", TestRunnableJob.class);
    jobTypes.put("TestCallableJob", TestCallableJob.class);
    final MapBasedJobFactory jobFactory = new MapBasedJobFactory(jobTypes);
    assertThat(jobFactory.getJobTypes()).isEqualTo(jobTypes);
    jobFactory.removeJobName("TestCallableJob");
    jobTypes.remove("TestCallableJob");
    assertThat(jobFactory.getJobTypes()).isEqualTo(jobTypes);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRemoveJobName_Null() {
    final Map<String, Class<?>> jobTypes =
        Map.of("TestRunnableJob", TestRunnableJob.class, "TestCallableJob", TestCallableJob.class);
    final MapBasedJobFactory jobFactory = new MapBasedJobFactory(jobTypes);
    assertThat(jobFactory.getJobTypes()).isEqualTo(jobTypes);
    jobFactory.removeJobName(null);
  }

  @Test
  public void testRemoveJobType() {
    final Map<String, Class<?>> jobTypes = new HashMap<String, Class<?>>();
    jobTypes.put("TestRunnableJob", TestRunnableJob.class);
    jobTypes.put("TestCallableJob", TestCallableJob.class);
    final MapBasedJobFactory jobFactory = new MapBasedJobFactory(jobTypes);
    assertThat(jobFactory.getJobTypes()).isEqualTo(jobTypes);
    jobFactory.removeJobType(TestCallableJob.class);
    jobTypes.values().remove(TestCallableJob.class);
    assertThat(jobFactory.getJobTypes()).isEqualTo(jobTypes);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRemoveJobType_Null() {
    final Map<String, Class<?>> jobTypes =
        Map.of("TestRunnableJob", TestRunnableJob.class, "TestCallableJob", TestCallableJob.class);
    final MapBasedJobFactory jobFactory = new MapBasedJobFactory(jobTypes);
    assertThat(jobFactory.getJobTypes()).isEqualTo(jobTypes);
    jobFactory.removeJobType(null);
  }

  @Test
  public void testMaterializeJob_Types() throws Exception {
    final Map<String, Class<?>> jobTypes =
        Map.of("TestRunnableJob", TestRunnableJob.class, "TestCallableJob", TestCallableJob.class);
    final MapBasedJobFactory jobFactory = new MapBasedJobFactory(jobTypes);
    final Object action = jobFactory.materializeJob(new Job("TestRunnableJob"));
    assertThat(action).isInstanceOf(TestRunnableJob.class);
    final Object action2 = jobFactory.materializeJob(new Job("TestCallableJob"));
    assertThat(action2).isInstanceOf(TestCallableJob.class);
  }

  @Test(expected = UnpermittedJobException.class)
  public void testMaterializeJob_Types_NotPermitted() throws Exception {
    final MapBasedJobFactory jobFactory = new MapBasedJobFactory(Map.of());
    jobFactory.materializeJob(new Job("TestRunnableJob"));
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
