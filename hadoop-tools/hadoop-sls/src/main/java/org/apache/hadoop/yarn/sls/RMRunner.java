/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.sls;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.TableMapping;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.ApplicationMasterLauncher;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.sls.conf.SLSConfiguration;
import org.apache.hadoop.yarn.sls.resourcemanager.MockAMLauncher;
import org.apache.hadoop.yarn.sls.resourcemanager.ControllableAMLauncher;
import org.apache.hadoop.yarn.sls.scheduler.SLSCapacityScheduler;
import org.apache.hadoop.yarn.sls.scheduler.SLSFairScheduler;
import org.apache.hadoop.yarn.sls.scheduler.SchedulerMetrics;
import org.apache.hadoop.yarn.sls.scheduler.SchedulerWrapper;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.hadoop.yarn.api.records.NodeState.RUNNING;

public class RMRunner {
  private ResourceManager rm;
  private String metricsOutputDir;
  private Configuration conf;
  private SLSRunner slsRunner;
  private String tableMapping;
  private Map<String, Integer> queueAppNumMap;
  private ScheduledExecutorService scheduler;
  private static final Logger LOG =
      org.slf4j.LoggerFactory.getLogger(RMRunner.class);

  public RMRunner(Configuration conf, SLSRunner slsRunner) {
    this.conf = conf;
    this.slsRunner = slsRunner;
    this.queueAppNumMap = new HashMap<>();
    this.scheduler = Executors.newScheduledThreadPool(1);
  }

  public void startRM() throws ClassNotFoundException, YarnException {
    Configuration rmConf = new YarnConfiguration(conf);
    String schedulerClass = rmConf.get(YarnConfiguration.RM_SCHEDULER);

    if (Class.forName(schedulerClass) == CapacityScheduler.class) {
      rmConf.set(YarnConfiguration.RM_SCHEDULER,
          SLSCapacityScheduler.class.getName());
      rmConf.setBoolean(YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS, true);
      rmConf.set(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES,
          ProportionalCapacityPreemptionPolicy.class.getName());
    } else if (Class.forName(schedulerClass) == FairScheduler.class) {
      rmConf.set(YarnConfiguration.RM_SCHEDULER,
          SLSFairScheduler.class.getName());
    } else if (Class.forName(schedulerClass) == FifoScheduler.class) {
      // TODO add support for FifoScheduler
      throw new YarnException("Fifo Scheduler is not supported yet.");
    }

    rmConf.setClass(
        CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
        TableMapping.class, DNSToSwitchMapping.class);
    rmConf.set(
        CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY,
        tableMapping);
    rmConf.set(SLSConfiguration.METRICS_OUTPUT_DIR, metricsOutputDir);

    // execute after delay time
    String excludeFilePath = conf.get(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH,
        YarnConfiguration.DEFAULT_RM_NODES_EXCLUDE_FILE_PATH);
    float nmDecommissionRatio = conf.getFloat(
        SLSConfiguration.NM_DECOMMISSION_RATIO, 0);
    if (excludeFilePath != null && !excludeFilePath.isEmpty()
        && nmDecommissionRatio > 0) {
      int nmDecommissionDelayMs = conf.getInt(
          SLSConfiguration.NM_DECOMMISSION_DELAY_MS, 0);
      LOG.info("Schedule decommission for nodes: ratio={}, delayMs={}",
          nmDecommissionRatio, nmDecommissionDelayMs);
      scheduler.schedule(() -> {
        decommissionNMs(excludeFilePath, nmDecommissionRatio);
      }, nmDecommissionDelayMs, TimeUnit.MILLISECONDS);
    }

    rm = new ResourceManager() {
      @Override
      protected ApplicationMasterLauncher createAMLauncher() {
        boolean controllableAMLauncherEnabled = conf.getBoolean(
            SLSConfiguration.CONTROLLABLE_AM_LAUNCHER_ENABLED, false);
        if (controllableAMLauncherEnabled) {
          return new ControllableAMLauncher(slsRunner, this.rmContext);
        }
        return new MockAMLauncher(slsRunner, this.rmContext);
      }
    };

    // Across runs of parametrized tests, the JvmMetrics objects is retained,
    // but is not registered correctly
    JvmMetrics jvmMetrics = JvmMetrics.initSingleton("ResourceManager", null);
    jvmMetrics.registerIfNeeded();

    // Init and start the actual ResourceManager
    rm.init(rmConf);
    rm.start();
  }

  // decommission NMs
  protected void decommissionNMs(String excludeFilePath, float ratio) {
    Map<NodeId, RMNode> nodes = rm.getRMContext().getRMNodes();
    List<String> toBeDecommissionedNodes = nodes.values().stream()
        .filter(node -> node.getState() == RUNNING)
        .filter(node -> Math.random() < ratio)
        .map(RMNode::getNodeID)
        .map(NodeId::getHost)
        .collect(Collectors.toList());
    LOG.info("Decommissioning {} NMs: totalNum={}, ratio={}, excludeFilePath={}",
        toBeDecommissionedNodes.size(), nodes.size(), ratio, excludeFilePath);
    try {
      // write nodeID into exclude-file
      Files.write(Paths.get(excludeFilePath), toBeDecommissionedNodes);
      // refresh nodes gracefully
      rm.getRMContext().getNodesListManager()
          .refreshNodes(rm.getConfig(), true);
      boolean cleared = Files.deleteIfExists(Paths.get(excludeFilePath));
      LOG.info("Done decommissioning {} NMs: totalNum={}, ratio={}, "
              + "excludeFilePath={}, cleared={}",
          toBeDecommissionedNodes.size(),
          nodes.size(), ratio, excludeFilePath, cleared);
    } catch (IOException | YarnException e) {
      LOG.error("Failed to write or refresh nodes", e);
    }
  }

  public void increaseQueueAppNum(String queue) throws YarnException {
    SchedulerWrapper wrapper = (SchedulerWrapper)rm.getResourceScheduler();
    String queueName = wrapper.getRealQueueName(queue);
    Integer appNum = queueAppNumMap.get(queueName);
    if (appNum == null) {
      appNum = 1;
    } else {
      appNum = appNum + 1;
    }

    queueAppNumMap.put(queueName, appNum);
    SchedulerMetrics metrics = wrapper.getSchedulerMetrics();
    if (metrics != null) {
      metrics.trackQueue(queueName);
    }
  }

  public void setMetricsOutputDir(String metricsOutputDir) {
    this.metricsOutputDir = metricsOutputDir;
  }

  public String getTableMapping() {
    return tableMapping;
  }

  public void setTableMapping(String tableMapping) {
    this.tableMapping = tableMapping;
  }

  public void stop() {
    rm.stop();
  }

  public ResourceManager getRm() {
    return rm;
  }

  public Map<String, Integer> getQueueAppNumMap() {
    return queueAppNumMap;
  }
}
