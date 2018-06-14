/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.store;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.StaticClusterAgentsFactory;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.BlobIdFactory;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.Config;
import com.github.ambry.config.Default;
import com.github.ambry.config.ServerConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.server.StoreKeyConverterFactoryImpl;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Throttler;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Consistency Checker tool is used to check for consistency
 * 1) Among replicas for any given partition
 * 2) In the index file boundaries on all replicas in a partition
 */
public class ConsistencyCheckerTool {
  /*
  Path referring to root directory containing one directory per replica
  Expected format of the path.of.input

  - Partition Root Directory
      - Replica_1
            - IndexSegment_0
            - IndexSegment_1
            .
            .
       - Replica_2
            - IndexSegment_0
            - IndexSegment_1
            .
            .
       .
       .
   User of this tool is expected to copy all index files for replicas of interest locally and run the tool with last
   modified times of the files unchanged. In linux use "cp -p" to maintain the file attributes.
   */

  /**
   * Config for the ConsistencyCheckerTool.
   */
  private static class ConsistencyCheckerToolConfig {

    /**
     * The path where the input is.
     */
    @Config("path.of.input")
    final File pathOfInput;

    /**
     * The path to the hardware layout file. Needed if using
     * {@link com.github.ambry.clustermap.StaticClusterAgentsFactory}.
     */
    @Config("hardware.layout.file.path")
    @Default("")
    final String hardwareLayoutFilePath;

    /**
     * The path to the partition layout file. Needed if using
     * {@link com.github.ambry.clustermap.StaticClusterAgentsFactory}.
     */
    @Config("partition.layout.file.path")
    @Default("")
    final String partitionLayoutFilePath;

    /**
     * A comma separated list of blob IDs that the tool should operate on. Leaving this empty indicates that the tool
     * should work on all blobs.
     */
    @Config("filter.set")
    @Default("")
    final Set<String> filterSet;

    /**
     * The number of index entries to process every second.
     */
    @Config("index.entries.to.process.per.sec")
    @Default("Long.MAX_VALUE")
    final long indexEntriesToProcessPerSec;

    /**
     * Constructs the configs associated with the tool.
     * @param verifiableProperties the props to use to load the config.
     */
    ConsistencyCheckerToolConfig(VerifiableProperties verifiableProperties) {
      pathOfInput = new File(verifiableProperties.getString("path.of.input"));
      hardwareLayoutFilePath = verifiableProperties.getString("hardware.layout.file.path", "");
      partitionLayoutFilePath = verifiableProperties.getString("partition.layout.file.path", "");
      String filterSetStr = verifiableProperties.getString("filter.set", "");
      if (!filterSetStr.isEmpty()) {
        filterSet = new HashSet<>(Arrays.asList(filterSetStr.split(",")));
      } else {
        filterSet = Collections.EMPTY_SET;
      }
      indexEntriesToProcessPerSec =
          verifiableProperties.getLongInRange("index.entries.to.process.per.sec", Long.MAX_VALUE, 1, Long.MAX_VALUE);
    }
  }

  private final DumpIndexTool dumpIndexTool;
  private final Set<StoreKey> filterSet;
  private final Throttler throttler;
  private final Time time;
  private final StoreKeyConverter storeKeyConverter;

  private static final Logger logger = LoggerFactory.getLogger(ConsistencyCheckerTool.class);

  public static void main(String args[]) throws Exception {
    VerifiableProperties properties = ToolUtils.getVerifiableProperties(args);
    ConsistencyCheckerToolConfig config = new ConsistencyCheckerToolConfig(properties);
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(properties);
    ServerConfig serverConfig = new ServerConfig(properties);
    try (ClusterMap clusterMap = new StaticClusterAgentsFactory(clusterMapConfig, config.hardwareLayoutFilePath,
        config.partitionLayoutFilePath).getClusterMap()) {
      StoreToolsMetrics metrics = new StoreToolsMetrics(clusterMap.getMetricRegistry());
      StoreConfig storeConfig = new StoreConfig(properties);
      // this tool supports only blob IDs. It can become generic if StoreKeyFactory provides a deserFromString method.
      BlobIdFactory blobIdFactory = new BlobIdFactory(clusterMap);
      Set<StoreKey> filterKeySet = new HashSet<>();
      for (String key : config.filterSet) {
        filterKeySet.add(new BlobId(key, clusterMap));
      }
      Time time = SystemTime.getInstance();
      Throttler throttler = new Throttler(config.indexEntriesToProcessPerSec, 1000, true, time);
      StoreKeyConverterFactory storeKeyConverterFactory = Utils.getObj(serverConfig.serverStoreKeyConverterFactory, properties, null);
      ConsistencyCheckerTool consistencyCheckerTool =
          new ConsistencyCheckerTool(clusterMap, blobIdFactory, storeConfig, filterKeySet, throttler, metrics, time, storeKeyConverterFactory.getStoreKeyConverter());
      boolean success =
          consistencyCheckerTool.checkConsistency(config.pathOfInput.listFiles(File::isDirectory)).getFirst();
      System.exit(success ? 0 : 1);
    }
  }

  public ConsistencyCheckerTool(ClusterMap clusterMap, StoreKeyFactory storeKeyFactory, StoreConfig storeConfig,
      Set<StoreKey> filterSet, Throttler throttler, StoreToolsMetrics metrics, Time time, StoreKeyConverter storeKeyConverter) {
    this.time = time;
    this.filterSet = filterSet;
    this.throttler = throttler;
    StoreMetrics storeMetrics = new StoreMetrics("ConsistencyCheckerTool", clusterMap.getMetricRegistry());
    dumpIndexTool = new DumpIndexTool(storeKeyFactory, storeConfig, time, metrics, storeMetrics);
    this.storeKeyConverter = storeKeyConverter;
  }

  /**
   * Checks for consistency b/w {@code replicas} by comparing their index processing results and determining replication
   * status.
   * @param replicas the replicas b/w which consistency has to be checked
   * @return a pair whose first element is a {@link Boolean} indicating consistency ({@code true} if consistent,
   * {@code false} otherwise) and the second element is a map from replica to the index processing results.
   * @throws Exception
   */
  public Pair<Boolean, Map<File, DumpIndexTool.IndexProcessingResults>> checkConsistency(File[] replicas)
      throws Exception {
    Pair<Boolean, Map<File, DumpIndexTool.IndexProcessingResults>> resultsByReplica =
        getIndexProcessingResults(replicas);
    boolean success = resultsByReplica.getFirst();
    if (success) {
      Map<StoreKey, ReplicationStatus> blobIdToStatusMap =
          getBlobStatusByReplica(replicas, resultsByReplica.getSecond());
      success = checkConsistency(blobIdToStatusMap, replicas.length).size() == 0;
    }
    return new Pair<>(success, resultsByReplica.getSecond());
  }

  /**
   * Processes the indexes of each of the replicas and returns the results.
   * @param replicas the replicas to process indexes for.
   * @return a {@link Pair} whose first indicates whether all results were sane and whose second contains the map of
   * individual results by replica.
   * @throws Exception if there is any error in processing the indexes.
   */
  private Pair<Boolean, Map<File, DumpIndexTool.IndexProcessingResults>> getIndexProcessingResults(File[] replicas)
      throws Exception {
    long currentTimeMs = time.milliseconds();
    Map<File, DumpIndexTool.IndexProcessingResults> results = new HashMap<>();
    boolean sane = true;
    for (File replica : replicas) {
      logger.info("Processing segment files for replica {} ", replica);
      DumpIndexTool.IndexProcessingResults result =
          dumpIndexTool.processIndex(replica, filterSet, currentTimeMs, throttler);
      sane = sane && result.isIndexSane();
      results.put(replica, result);
    }
    return new Pair<>(sane, results);
  }

  /**
   * Takes all the StoreKeys from the file replicas and runs them
   * through the StoreKeyConverter in a batch operation
   * @param replicas An Array of replica directories from which blobIds need to be collected
   * @param results the results of processing the indexes of the given {@code replicas}.
   * @return mapping of original keys to converted keys.  If there's no converted equivalent
   * the value will be the same as the key.
   * @throws Exception
   */
  private Map<StoreKey, StoreKey> createConversionKeyMap(File[] replicas,
    Map<File, DumpIndexTool.IndexProcessingResults> results) throws Exception {
    List<StoreKey> storeKeys = new ArrayList<>();
    for (File replica : replicas) {
          DumpIndexTool.IndexProcessingResults result = results.get(replica);
          for (Map.Entry<StoreKey, DumpIndexTool.Info> entry : result.getKeyToState().entrySet()) {
            storeKeys.add(entry.getKey());
          }
     }
     return storeKeyConverter.convert(storeKeys);
  }

  /**
   * Walks through all replicas and collects blob status in each of them.
   * @param replicas An Array of replica directories from which blob status' need to be collected
   * @param results the results of processing the indexes of the given {@code replicas}.
   * @return a {@link Map} of BlobId to {@link ReplicationStatus}.  If key has a conversion
   * equivalent (vis a vis the storeKeyConverter), the map key will be of that converted equivalent
   * @throws Exception
   */
  private Map<StoreKey, ReplicationStatus> getBlobStatusByReplica(File[] replicas,
      Map<File, DumpIndexTool.IndexProcessingResults> results) throws Exception {
    Map<StoreKey, ReplicationStatus> keyReplicationStatusMap = new HashMap<>();
    Map<StoreKey, StoreKey> convertMap = createConversionKeyMap(replicas, results);
    for (File replica : replicas) {
      DumpIndexTool.IndexProcessingResults result = results.get(replica);
      for (Map.Entry<StoreKey, DumpIndexTool.Info> entry : result.getKeyToState().entrySet()) {
        StoreKey key = entry.getKey();
        key = convertMap.get(key);
        if (!keyReplicationStatusMap.containsKey(key)) {
          keyReplicationStatusMap.put(key, new ReplicationStatus(replicas));
        }
        ReplicationStatus status = keyReplicationStatusMap.get(key);
        DumpIndexTool.Info info = entry.getValue();
        status.setBelongsToRecentIndexSegment(info.isInRecentIndexSegment());
        if (info.getStates().contains(DumpIndexTool.BlobState.Valid)) {
          status.addAvailable(replica);
        } else {
          status.addDeletedOrExpired(replica);
        }
      }
    }
    return keyReplicationStatusMap;
  }

  /**
   * Walks through blobs and its status in all replicas and collects inconsistent blobs information
   * @param blobIdToStatusMap {@link Map} of BlobId to {@link ReplicationStatus} that needs to be updated with the
   *                                         status of every blob in the index
   * @param replicaCount total replica count
   * @return {@link List} of real inconsistent blobIds
   */
  private List<StoreKey> checkConsistency(Map<StoreKey, ReplicationStatus> blobIdToStatusMap, int replicaCount) {
    List<StoreKey> realInconsistentBlobs = new ArrayList<>();
    logger.info("Total Blobs Found {}", blobIdToStatusMap.size());
    long totalInconsistentBlobs = 0;
    long inconsistentDueToReplicationCount = 0;
    long acceptableInconsistentBlobs = 0;
    for (StoreKey blobId : blobIdToStatusMap.keySet()) {
      ReplicationStatus consistencyBlobResult = blobIdToStatusMap.get(blobId);
      // valid blobs : count of available replicas = total replica count or count of deleted replicas = total replica count
      boolean isValid = consistencyBlobResult.available.size() == replicaCount
          || consistencyBlobResult.deletedOrExpired.size() == replicaCount;
      if (!isValid) {
        totalInconsistentBlobs++;
        if ((consistencyBlobResult.deletedOrExpired.size() + consistencyBlobResult.unavailable.size()
            == replicaCount)) {
          // acceptable inconsistent blobs : count of deleted + count of unavailable = total replica count
          logger.debug("Partially deleted (acceptable inconsistency) blob {} isDeletedOrExpired {}. Blob status - {}",
              blobId, consistencyBlobResult.isDeletedOrExpired, consistencyBlobResult);
          acceptableInconsistentBlobs++;
        } else {
          if (consistencyBlobResult.belongsToRecentIndexSegment) {
            logger.debug("Inconsistent blob found possibly due to replication {} Status map {} ", blobId,
                consistencyBlobResult);
            inconsistentDueToReplicationCount++;
          } else {
            logger.error("Inconsistent blob found {} Status map {}", blobId, consistencyBlobResult);
            realInconsistentBlobs.add(blobId);
          }
        }
      }
    }
    // Inconsistent blobs = real inconsistent + acceptable inconsistent + inconsistent due to replication Lag
    // Acceptable inconsistent = due to deletion, some replicas reports as deleted, whereas some reports as unavailable
    // Inconsistent due to replication lag = Inconsistency due to replication lag.
    // Anything else is considered to be real inconsistent blobs
    logger.info("Total Inconsistent blobs count : {}", totalInconsistentBlobs);
    logger.info("Acceptable Inconsistent blobs count : {}", acceptableInconsistentBlobs);
    logger.info("Inconsistent blobs count due to replication lag : {}", inconsistentDueToReplicationCount);
    logger.info("Real Inconsistent blobs count : {} ", realInconsistentBlobs.size());
    return realInconsistentBlobs;
  }

  /**
   * Contains status of a blob across all replicas, whether it is deleted or expired in any of them and whether it
   * belongs to the most recent segment in any of them.
   */
  private static class ReplicationStatus {
    final Set<File> available = new HashSet<>();
    final Set<File> deletedOrExpired = new HashSet<>();
    final Set<File> unavailable = new HashSet<>();
    boolean isDeletedOrExpired;
    boolean belongsToRecentIndexSegment = false;

    /**
     * Initializes a {@link ReplicationStatus} with a list of Replica. ConsistencyChecker uses the
     * {@link ReplicationStatus} to keep track of the status of a blob in every replica.
     * @param replicas the list of replicas for which blob status needs to be collected
     */
    ReplicationStatus(File[] replicas) {
      unavailable.addAll(Arrays.asList(replicas));
    }

    void addAvailable(File replica) {
      available.add(replica);
      unavailable.remove(replica);
    }

    void setBelongsToRecentIndexSegment(boolean belongsToRecentIndexSegment) {
      this.belongsToRecentIndexSegment = this.belongsToRecentIndexSegment || belongsToRecentIndexSegment;
    }

    void addDeletedOrExpired(File replica) {
      deletedOrExpired.add(replica);
      isDeletedOrExpired = true;
      unavailable.remove(replica);
      available.remove(replica);
    }

    @Override
    public String toString() {
      int totalReplicas = available.size() + deletedOrExpired.size() + unavailable.size();
      return "Available size: " + available.size() + ", Available :: " + available + "\nDeleted/Expired size: "
          + deletedOrExpired.size() + " Deleted/Expired :: " + deletedOrExpired + "\nUnavailable size: "
          + unavailable.size() + " Unavailable :: " + unavailable + "\nTotal Replica count: " + totalReplicas;
    }
  }
}
