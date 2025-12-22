/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.PartitionQueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;

@Metrics(context = "yarn")
public class CSPartitionQueueMetrics extends PartitionQueueMetrics {

  @Metric("Guaranteed memory in MB")
  MutableGaugeLong guaranteedMB;
  @Metric("Guaranteed CPU in virtual cores")
  MutableGaugeInt guaranteedVCores;
  @Metric("Maximum memory in MB")
  MutableGaugeLong maxCapacityMB;
  @Metric("Maximum CPU in virtual cores")
  MutableGaugeInt maxCapacityVCores;

  public CSPartitionQueueMetrics(MetricsSystem ms, String queueName,
      Queue parent, boolean enableUserMetrics, Configuration conf,
      String partition) {
    super(ms, queueName, parent, enableUserMetrics, conf, partition);
  }

  public long getGuaranteedMB() {
    return guaranteedMB.value();
  }

  public int getGuaranteedVCores() {
    return guaranteedVCores.value();
  }

  public void setGuaranteedResources(Resource res) {
      guaranteedMB.set(res.getMemorySize());
      guaranteedVCores.set(res.getVirtualCores());
  }

  public long getMaxCapacityMB() {
    return maxCapacityMB.value();
  }

  public int getMaxCapacityVCores() {
    return maxCapacityVCores.value();
  }

  public void setMaxCapacityResources(Resource res) {
      maxCapacityMB.set(res.getMemorySize());
      maxCapacityVCores.set(res.getVirtualCores());
  }
}
