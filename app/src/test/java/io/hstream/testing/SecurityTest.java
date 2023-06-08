package io.hstream.testing;

import static org.assertj.core.api.Assertions.*;

import io.hstream.HStreamClient;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

/* TLS cases
Tag("tls"): enable tls in servers and client
Tag("tls-authentication"): enable tls authentication in servers and client
 */
@Tag("basicTest")
@ExtendWith(ClusterExtension.class)
public class SecurityTest {
  @Test
  @Timeout(20)
  @Tag("tls")
  void testTls() {}

  @Test
  @Timeout(20)
  @Tag("tls")
  @Tag("tls-authentication")
  void testTlsAuthentication() {}

  @Test
  @Timeout(20)
  void testUntrustedServer() {
    String caPath = getClass().getClassLoader().getResource("security/ca.cert.pem").getPath();
    assertThatThrownBy(() -> HStreamClient.builder().enableTls().tlsCaPath(caPath).build())
        .isInstanceOf(Exception.class);
  }

  @Test
  @Timeout(20)
  @Tag("tls")
  @Tag("tls-authentication")
  void testUntrustedClient() {
    String caPath = getClass().getClassLoader().getResource("security/ca.cert.pem").getPath();
    assertThatThrownBy(() -> HStreamClient.builder().enableTls().tlsCaPath(caPath).build())
        .isInstanceOf(Exception.class);
  }
}
