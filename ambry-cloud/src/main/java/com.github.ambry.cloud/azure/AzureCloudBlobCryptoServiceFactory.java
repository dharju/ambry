package com.github.ambry.cloud.azure;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.cloud.CloudBlobCryptoService;
import com.github.ambry.cloud.CloudBlobCryptoServiceFactory;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.router.CryptoServiceFactory;
import com.github.ambry.router.KeyManagementServiceFactory;
import com.github.ambry.utils.Utils;
import java.security.GeneralSecurityException;


public class AzureCloudBlobCryptoServiceFactory implements CloudBlobCryptoServiceFactory {

  private CryptoServiceFactory cryptoServiceFactory;
  private KeyManagementServiceFactory keyManagementServiceFactory;
  private String context;

  public AzureCloudBlobCryptoServiceFactory(VerifiableProperties verifiableProperties, String clusterName, MetricRegistry metricRegistry)
      throws ReflectiveOperationException {
    CloudConfig cloudConfig = new CloudConfig(verifiableProperties);
    cryptoServiceFactory = Utils.getObj(cloudConfig.cryptoServiceFactoryClass, verifiableProperties, metricRegistry);
    keyManagementServiceFactory = Utils.getObj(cloudConfig.kmsServiceFactoryClass, verifiableProperties, clusterName, metricRegistry);
    context = cloudConfig.kmsServiceKeyContext;
  }

  @Override
  public CloudBlobCryptoService getCloudBlobCryptoService() {
    try {
      return new AzureCloudBlobCryptoService(cryptoServiceFactory.getCryptoService(), keyManagementServiceFactory.getKeyManagementService(), context);
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException(e);
    }
  }
}
