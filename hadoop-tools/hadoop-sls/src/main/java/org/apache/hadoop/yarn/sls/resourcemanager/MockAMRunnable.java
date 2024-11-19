package org.apache.hadoop.yarn.sls.resourcemanager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncher;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.sls.SLSRunner;
import org.apache.hadoop.yarn.sls.appmaster.AMSimulator;
import org.apache.hadoop.yarn.sls.conf.SLSConfiguration;
import org.apache.hadoop.yarn.sls.nodemanager.NMSimulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MockAMRunnable implements Runnable {

  private static final Logger LOG =
      LoggerFactory.getLogger(MockAMRunnable.class);

  private ContainerManagementProtocol containerMgrProxy;

  protected SLSRunner slsRunner;
  protected RMAppAttempt application;
  protected Configuration conf;
  protected AMLauncherEventType eventType;
  protected RMContext rmContext;

  protected EventHandler handler;
  protected long cleanupDelayedMs;

  public MockAMRunnable(SLSRunner slsRunner, RMContext rmContext,
      RMAppAttempt application,
      AMLauncherEventType eventType, Configuration conf) {
    this.slsRunner = slsRunner;
    this.application = application;
    this.conf = conf;
    this.eventType = eventType;
    this.rmContext = rmContext;
    this.handler = rmContext.getDispatcher().getEventHandler();
    this.cleanupDelayedMs = conf.getLong(
        SLSConfiguration.NM_SLOW_CLEANUP_DELAY_MS, 0);
  }


  private void setupAMRMToken(RMAppAttempt appAttempt) {
    // Setup AMRMToken
    Token<AMRMTokenIdentifier> amrmToken =
        this.rmContext.getAMRMTokenSecretManager().createAndGetAMRMToken(
            appAttempt.getAppAttemptId());
    ((RMAppAttemptImpl) appAttempt).setAMRMToken(amrmToken);
  }

  @SuppressWarnings("unchecked") public void run() {
    ApplicationId appId = application.getAppAttemptId().getApplicationId();
    AMSimulator ams = slsRunner.getAMSimulatorByAppId(appId);
    if (ams == null) {
      throw new YarnRuntimeException(
          "Didn't find any AMSimulator for applicationId=" + appId);
    }
    Container amContainer = application.getMasterContainer();
    switch (eventType) {
    case LAUNCH:
      try {
        setupAMRMToken(application);
        // Notify RMAppAttempt to change state
        this.rmContext.getDispatcher().getEventHandler().handle(
            new RMAppAttemptEvent(application.getAppAttemptId(),
                RMAppAttemptEventType.LAUNCHED));

        ams.notifyAMContainerLaunched(
            application.getMasterContainer());
        LOG.info("Notify AM launcher launched:" + amContainer.getId());

        slsRunner.getNmMap().get(amContainer.getNodeId())
            .addNewContainer(amContainer, -1, appId);
        ams.getRanNodes().add(amContainer.getNodeId());
        return;
      } catch (Exception e) {
        throw new YarnRuntimeException(e);
      }
    case CLEANUP:
      NMSimulator nm = slsRunner.getNmMap().get(amContainer.getNodeId());
      LOG.info("Cleaning master {} on {} with slowState={} and delayMs={}",
          application.getAppAttemptId(), nm.getNode().getNodeID(),
          nm.isSlow(), cleanupDelayedMs);
      if (nm.isSlow() && cleanupDelayedMs>0) {
        try {
          Thread.sleep(cleanupDelayedMs);
        } catch (InterruptedException e) {
          LOG.warn("Interrupted while waiting for cleanup-delayMs", e);
        }
      }
      nm.cleanupContainer(amContainer.getId());
      LOG.info("Done cleaning master {} on {} with slowState={}",
          application.getAppAttemptId(), nm.getNode().getNodeID(), nm.isSlow());
      break;
    default:
      LOG.warn("Received unknown event-type " + eventType + ". Ignoring.");
      break;
    }
  }
}
