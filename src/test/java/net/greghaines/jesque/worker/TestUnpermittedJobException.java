package net.greghaines.jesque.worker;

import static com.google.common.truth.Truth.assertThat;

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
    assertThat(upje.getMessage()).isEqualTo(message);
  }

  @Test
  public void testGetType() {
    final Class<?> type = String.class;
    final UnpermittedJobException upje = new UnpermittedJobException(type);
    assertThat(upje.getType()).isEqualTo(type);
  }
}
