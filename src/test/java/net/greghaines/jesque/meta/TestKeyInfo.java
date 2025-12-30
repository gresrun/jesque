package net.greghaines.jesque.meta;

import static com.google.common.truth.Truth.assertThat;

import java.util.List;
import org.junit.Test;

public class TestKeyInfo {

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_NullKey() {
    new KeyInfo(null, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor_BadKey() {
    new KeyInfo("foo", null);
  }

  @Test
  public void testConstructor() {
    final KeyType type = KeyType.HASH;
    final KeyInfo keyInfo = new KeyInfo("foo:bar:baz:qux", type);
    assertThat(keyInfo.getNamespace()).isEqualTo("foo");
    assertThat(keyInfo.getName()).isEqualTo("bar:baz:qux");
    assertThat(keyInfo.getType()).isEqualTo(type);
  }

  @Test
  public void testProperties() {
    final KeyInfo keyInfo = new KeyInfo();
    final String name = "foo";
    keyInfo.setName(name);
    assertThat(keyInfo.getName()).isEqualTo(name);
    assertThat(keyInfo.toString()).isEqualTo(name);
    final String namespace = "bar";
    keyInfo.setNamespace(namespace);
    assertThat(keyInfo.getNamespace()).isEqualTo(namespace);
    final KeyType type = KeyType.HASH;
    keyInfo.setType(type);
    assertThat(keyInfo.getType()).isEqualTo(type);
    final Long size = 3l;
    keyInfo.setSize(size);
    assertThat(keyInfo.getSize()).isEqualTo(size);
    final List<String> arrayValue = List.of("foo", "bar");
    keyInfo.setArrayValue(arrayValue);
    assertThat(keyInfo.getArrayValue()).isEqualTo(arrayValue);
  }

  @Test
  public void testCompareToEqualsHashCode() {
    final KeyInfo ki1 = new KeyInfo();
    assertThat(ki1.compareTo(null)).isGreaterThan(0);
    assertThat(ki1.equals(null)).isFalse();
    assertThat(ki1).isEqualTo(ki1);
    final KeyInfo ki2 = new KeyInfo();
    assertThat(ki1).isEquivalentAccordingToCompareTo(ki2);
    assertThat(ki1).isEqualTo(ki2);
    assertThat(ki1.hashCode()).isEqualTo(ki2.hashCode());
    ki1.setName("foo");
    assertThat(ki1).isGreaterThan(ki2);
    assertThat(ki1).isNotEqualTo(ki2);
    ki1.setName(null);
    ki2.setName("foo");
    assertThat(ki1).isLessThan(ki2);
    assertThat(ki1).isNotEqualTo(ki2);
    ki1.setName("foo");
    assertThat(ki1).isEquivalentAccordingToCompareTo(ki2);
    assertThat(ki1).isEqualTo(ki2);
    assertThat(ki1.hashCode()).isEqualTo(ki2.hashCode());
    ki1.setName("bar");
    assertThat(ki1).isLessThan(ki2);
    assertThat(ki1).isNotEqualTo(ki2);
    ki1.setName("qux");
    assertThat(ki1).isGreaterThan(ki2);
    assertThat(ki1).isNotEqualTo(ki2);
  }
}
