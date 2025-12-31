package net.greghaines.jesque.worker;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import java.util.concurrent.Callable;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.utils.JesqueUtils;
import org.junit.Test;

/** Tests ReflectiveJobFactory. */
public class TestReflectiveJobFactory {

  @Test
  public void testMaterializeJob() throws Exception {
    final ReflectiveJobFactory jobFactory = new ReflectiveJobFactory();
    final Object action = jobFactory.materializeJob(new Job(TestRunnableJob.class.getName()));
    assertThat(action).isInstanceOf(TestRunnableJob.class);
    final Object action2 = jobFactory.materializeJob(new Job(TestCallableJob.class.getName()));
    assertThat(action2).isInstanceOf(TestCallableJob.class);
  }

  @Test
  public void testMaterializeJob_NotRunnable() throws Exception {
    assertThrows(
        ClassCastException.class,
        () -> {
          JesqueUtils.materializeJob(new Job(TestBadJob.class.getName()));
        });
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
