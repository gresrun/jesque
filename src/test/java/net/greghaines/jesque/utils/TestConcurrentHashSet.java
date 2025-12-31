package net.greghaines.jesque.utils;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.junit.Test;

public class TestConcurrentHashSet {

  @Test
  public void testSingleElem() {
    final ConcurrentHashSet<String> set = new ConcurrentHashSet<>();
    final String foo = "foo";
    assertThat(set).isEmpty();
    assertThat(set).doesNotContain(foo);
    assertThat(set.remove(foo)).isFalse();

    assertThat(set.add(foo)).isTrue();
    assertThat(set.add(foo)).isFalse();
    assertThat(set).hasSize(1);
    assertThat(set).contains(foo);

    final Object[] arr1 = set.toArray();
    assertThat(arr1).isNotNull();
    assertThat(arr1).isEqualTo(new Object[] {foo});

    final String[] arr2 = new String[set.size()];
    final String[] arr3 = set.toArray(arr2);
    assertThat(arr3).isNotNull();
    assertThat(arr3).isSameInstanceAs(arr2);
    assertThat(arr3).isEqualTo(new String[] {foo});

    assertThat(set.remove(foo)).isTrue();
    assertThat(set.remove(foo)).isFalse();
    assertThat(set).doesNotContain(foo);
    assertThat(set).isEmpty();
  }

  @Test
  public void testMultiElem() {
    final ConcurrentHashSet<String> set = new ConcurrentHashSet<>(4);
    final String foo = "foo";
    final String bar = "bar";
    final String baz = "baz";
    final String qux = "qux";
    final List<String> fullList = List.of(foo, bar, baz, qux);
    final List<String> noQux = List.of(foo, bar, baz);
    final List<String> noFooBar = List.of(baz, qux);

    assertThat(set.addAll(fullList)).isTrue();
    assertThat(set.addAll(noQux)).isFalse();
    assertThat(set).isNotEmpty();
    assertThat(set).hasSize(fullList.size());
    assertThat(set.containsAll(fullList)).isTrue();
    assertThat(set).contains(foo);
    assertThat(set).contains(bar);
    assertThat(set).contains(baz);
    assertThat(set).contains(qux);

    final Iterator<String> iter = set.iterator();
    assertThat(iter).isNotNull();
    int i = 0;
    for (final String elem : set) {
      if (elem != null) {
        i++;
      }
    }
    assertThat(i).isEqualTo(set.size());

    assertThat(set.retainAll(noQux)).isTrue();
    assertThat(set).hasSize(noQux.size());
    assertThat(set).containsExactlyElementsIn(noQux);
    assertThat(set).contains(foo);
    assertThat(set).contains(bar);
    assertThat(set).contains(baz);
    assertThat(set).doesNotContain(qux);
    assertThat(set.removeAll(noFooBar)).isTrue();
    assertThat(set).hasSize(2);
    assertThat(set).contains(foo);
    assertThat(set).contains(bar);
    assertThat(set).doesNotContain(baz);
    assertThat(set).doesNotContain(qux);

    set.clear();
    assertThat(set).isEmpty();
  }

  @Test
  public void testCollectionConstructor_Null() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          new ConcurrentHashSet<String>(null);
        });
  }

  @Test
  public void testCollectionConstructor() {
    final List<String> fullList = Arrays.asList("foo", "bar", "baz", "qux");
    final ConcurrentHashSet<String> set = new ConcurrentHashSet<>(fullList);
    assertThat(set).hasSize(fullList.size());
    assertThat(set).containsExactlyElementsIn(fullList);
  }

  @Test
  public void testOtherConstructors() {
    assertThat(new ConcurrentHashSet<String>(4, 0.75f)).isNotNull();
    assertThat(new ConcurrentHashSet<String>(4, 0.75f, 2)).isNotNull();
  }
}
