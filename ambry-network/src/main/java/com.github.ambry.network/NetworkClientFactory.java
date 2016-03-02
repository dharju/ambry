package com.github.ambry.network;

import com.github.ambry.config.NetworkConfig;
import com.github.ambry.utils.Time;
import java.io.IOException;


/**
 * A factory class used to get new instances of a {@link NetworkClient}
 */
class NetworkClientFactory {
  private final NetworkMetrics networkMetrics;
  private final NetworkRequestMetrics networkRequestMetrics;
  private final NetworkConfig networkConfig;
  private final SSLFactory sslFactory;
  private final int maxConnectionsPerPortPlainText;
  private final int maxConnectionsPerPortSsl;
  private final int connectionCheckoutTimeoutMs;
  private final Time time;

  /**
   * Construct a factory using the given parameters.
   * @param networkMetrics the metrics for the Network layer.
   * @param networkConfig the configs for the Network layer.
   * @param sslFactory the sslFactory used for SSL connections.
   * @param maxConnectionsPerPortPlainText the max number of ports per plain text port for this connection manager.
   * @param maxConnectionsPerPortSsl the max number of ports per ssl port for this connection manager.
   * @param time the Time instance to use.
   */
  NetworkClientFactory(NetworkMetrics networkMetrics, NetworkRequestMetrics networkRequestMetrics,
      NetworkConfig networkConfig, SSLFactory sslFactory, int maxConnectionsPerPortPlainText,
      int maxConnectionsPerPortSsl, int connectionCheckoutTimeoutMs, Time time) {
    this.networkMetrics = networkMetrics;
    this.networkRequestMetrics = networkRequestMetrics;
    this.networkConfig = networkConfig;
    this.sslFactory = sslFactory;
    this.maxConnectionsPerPortPlainText = maxConnectionsPerPortPlainText;
    this.maxConnectionsPerPortSsl = maxConnectionsPerPortSsl;
    this.connectionCheckoutTimeoutMs = connectionCheckoutTimeoutMs;
    this.time = time;
  }

  /**
   * Construct and return a new {@link NetworkClient}
   * @return return a new {@link NetworkClient}
   * @throws IOException if the {@link Selector} could not be instantiated.
   */
  NetworkClient getNetworkClient()
      throws IOException {
    Selector selector = new Selector(networkMetrics, time, sslFactory);
    ConnectionTracker connectionTracker =
        new ConnectionTracker(maxConnectionsPerPortPlainText, maxConnectionsPerPortSsl, time);
    return new NetworkClient(selector, connectionTracker, networkConfig, networkRequestMetrics,
        connectionCheckoutTimeoutMs, time);
  }
}

