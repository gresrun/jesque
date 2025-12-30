package net.greghaines.jesque.meta;

import static com.google.common.truth.Truth.assertThat;

import java.util.Locale;
import org.junit.Test;

public class TestKeyType {

  @Test
  public void testToString() {
    for (final KeyType keyType : KeyType.values()) {
      assertThat(keyType.toString()).isEqualTo(keyType.name().toLowerCase(Locale.US));
    }
  }

  @Test
  public void testGetKeyTypeByValue() {
    for (final KeyType keyType : KeyType.values()) {
      assertThat(KeyType.getKeyTypeByValue(keyType.name().toLowerCase(Locale.US)))
          .isEqualTo(keyType);
    }
  }
}
