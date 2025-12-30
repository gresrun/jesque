package net.greghaines.jesque.utils;

import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;

public class TestNoSuchConstructorException {

  @Test
  public void testStringConstructor() {
    final NoSuchConstructorException nsce = new NoSuchConstructorException("foo");
    assertThat(nsce.getType()).isNull();
    assertThat(nsce.getArgs()).isNull();
  }

  @Test
  public void testClassArgsConstructor() {
    final NoSuchConstructorException nsce =
        new NoSuchConstructorException(TestNoSuchConstructorException.class, "foo", "bar");
    assertThat(nsce.getType()).isEqualTo(TestNoSuchConstructorException.class);
    assertThat(nsce.getArgs()).isEqualTo(new String[] {"foo", "bar"});
  }
}
