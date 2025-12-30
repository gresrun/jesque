package net.greghaines.jesque.worker;

import static com.google.common.truth.Truth.assertThat;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.TestAction;
import org.junit.Test;

/**
 * Tests WorkerImplFactory.
 *
 * @author Greg Haines
 */
public class TestWorkerImplFactory {

  @Test
  public void testCall() {
    final Collection<String> queues = Arrays.asList("foo", "bar");
    final Map<String, Class<?>> jobTypes = new LinkedHashMap<String, Class<?>>(1);
    jobTypes.put("test", TestAction.class);
    final MapBasedJobFactory jobFactory = new MapBasedJobFactory(jobTypes);
    final WorkerImplFactory factory =
        new WorkerImplFactory(Config.getDefaultConfig(), queues, jobFactory);
    final WorkerImpl worker = factory.call();
    assertThat(worker).isNotNull();
    assertThat(worker.getQueues()).containsExactlyElementsIn(queues);
    assertThat(worker.getJobFactory()).isEqualTo(jobFactory);
  }
}
