package net.greghaines.jesque.utils;

import static com.google.common.truth.Truth.assertThat;

import java.lang.reflect.Constructor;
import java.util.Set;
import org.junit.Test;

public class TestAmbiguousConstructorException {

  @Test
  public void testStringConstructor() {
    final AmbiguousConstructorException ace = new AmbiguousConstructorException("foo");
    assertThat(ace.getType()).isNull();
    assertThat(ace.getArgs()).isNull();
  }

  @Test
  public void testClassArgsOptionsConstructor() {
    final Set<Constructor<?>> options = Set.of();
    final AmbiguousConstructorException ace =
        new AmbiguousConstructorException(
            TestAmbiguousConstructorException.class, new Object[] {"foo", "bar"}, options);
    assertThat(ace.getType()).isEqualTo(TestAmbiguousConstructorException.class);
    assertThat(ace.getArgs()).isEqualTo(new Object[] {"foo", "bar"});
    assertThat(ace.getOptions()).isEqualTo(options);
  }
}
