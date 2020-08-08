package io.lubricant.consensus.raft.support;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 配置
 */
public class RaftConfig {

    private final URI localURI;
    private final List<URI> remoteURIs;
    private final boolean preVote;
    private final int tick;
    private final double heartbeat, election, broadcast;
    private final String logPath, statePath, lockerPath, snapshotPath;
    private final int triggerInterval, maintainInterval, compactInterval, stateChangeThreshold, dirtyLogTolerance;
    private final int availableCriticalPoint, recoveryCoolDownMills;

    public RaftConfig(URI localURI, List<URI> remoteURIs, boolean preVote,
                      int tick, double heartbeat, double election, double broadcast,
                      String logDir, String stateDir, String lockerDir, String snapshotDir,
                      int triggerInterval, int maintainInterval, int compactInterval, int stateChangeThreshold, int dirtyLogTolerance,
                      int availableCriticalPoint, int recoveryCoolDownMills) {
        this.localURI = localURI;
        this.remoteURIs = remoteURIs;
        this.preVote = preVote;
        this.tick = tick;
        this.heartbeat = heartbeat;
        this.election = election;
        this.broadcast = broadcast;
        this.triggerInterval = triggerInterval;
        this.maintainInterval = maintainInterval;
        this.compactInterval = compactInterval;
        this.stateChangeThreshold = stateChangeThreshold;
        this.dirtyLogTolerance = dirtyLogTolerance;
        this.availableCriticalPoint = availableCriticalPoint;
        this.recoveryCoolDownMills = recoveryCoolDownMills;
        this.logPath = logDir + (logDir.endsWith(File.separator) ? "": File.separator);
        this.statePath = stateDir + (stateDir.endsWith(File.separator) ? "": File.separator);
        this.lockerPath = lockerDir + (lockerDir.endsWith(File.separator) ? "": File.separator);
        this.snapshotPath = snapshotDir + (lockerDir.endsWith(File.separator) ? "": File.separator);
    }

    public static RaftConfig loadXmlConfig(String configPath, boolean classpath) throws Exception {

        URL resource = ! classpath ? Paths.get(configPath).toUri().toURL():
                Thread.currentThread().getContextClassLoader().getResource(configPath);

        if (resource == null)
            throw new FileNotFoundException(configPath);
        else
            try (InputStream stream = resource.openStream()) {

            Element config = DocumentBuilderFactory.
                    newInstance().newDocumentBuilder().
                    parse(stream).getDocumentElement();

            if (!"raft".equals(config.getTagName())) {
                throw new IllegalArgumentException("raft");
            }

            XPath xPath = XPathFactory.newInstance().newXPath();

            Number localNum = (Number) xPath.evaluate("count(cluster/local)", config, XPathConstants.NUMBER);
            Number remoteNum = (Number) xPath.evaluate("count(cluster/remote)", config, XPathConstants.NUMBER);
            if (localNum.intValue() != 1 || remoteNum.intValue() < 1 ||
                    localNum.intValue() + remoteNum.intValue() % 2 == 0) {
                throw new IllegalArgumentException("cluster");
            }

            String local = (String) xPath.evaluate("cluster/local", config, XPathConstants.STRING);
            NodeList remoteNodes = (NodeList) xPath.evaluate("cluster/remote", config, XPathConstants.NODESET);
            List<String> remote = new ArrayList<>(remoteNodes.getLength());
            for (int i=0; i<remoteNodes.getLength(); i++) {
                remote.add(remoteNodes.item(i).getTextContent());
            }

            boolean preVote = false;
            if (((Number) xPath.evaluate("count(timeout/pre-vote)", config, XPathConstants.NUMBER)).intValue() == 1) {
                preVote = (Boolean)xPath.evaluate("timeout/pre-vote", config, XPathConstants.BOOLEAN);
            }

            if (((Number) xPath.evaluate("count(timeout/tick)", config, XPathConstants.NUMBER)).intValue() != 1 ||
                ((Number) xPath.evaluate("count(timeout/heartbeat)", config, XPathConstants.NUMBER)).intValue() != 1||
                ((Number) xPath.evaluate("count(timeout/election)", config, XPathConstants.NUMBER)).intValue() != 1||
                ((Number) xPath.evaluate("count(timeout/broadcast)", config, XPathConstants.NUMBER)).intValue() != 1) {
                throw new IllegalArgumentException("timeout");
            }

            int tick = ((Number)xPath.evaluate("timeout/tick", config, XPathConstants.NUMBER)).intValue();
            double heartbeat = ((Number)xPath.evaluate("timeout/heartbeat", config, XPathConstants.NUMBER)).doubleValue();
            double election = ((Number)xPath.evaluate("timeout/election", config, XPathConstants.NUMBER)).doubleValue();
            double broadcast = ((Number)xPath.evaluate("timeout/broadcast", config, XPathConstants.NUMBER)).doubleValue();
            if (tick <= 0 || heartbeat <= 0 || election <= 0 || broadcast <= 0) {
                throw new IllegalArgumentException("timeout");
            }
            if (heartbeat >= election || broadcast >= heartbeat) {
                throw new IllegalArgumentException("timeout");
            }

            if (((Number) xPath.evaluate("count(snapshot/trigger-interval)", config, XPathConstants.NUMBER)).intValue() != 1 ||
                ((Number) xPath.evaluate("count(snapshot/maintain-interval)", config, XPathConstants.NUMBER)).intValue() != 1 ||
                ((Number) xPath.evaluate("count(snapshot/compact-interval)", config, XPathConstants.NUMBER)).intValue() != 1||
                ((Number) xPath.evaluate("count(snapshot/state-change-threshold)", config, XPathConstants.NUMBER)).intValue() != 1||
                ((Number) xPath.evaluate("count(snapshot/dirty-log-tolerance)", config, XPathConstants.NUMBER)).intValue() != 1) {
                throw new IllegalArgumentException("snapshot");
            }

            int triggerInterval = ((Number)xPath.evaluate("snapshot/trigger-interval", config, XPathConstants.NUMBER)).intValue();
            int maintainInterval = ((Number)xPath.evaluate("snapshot/maintain-interval", config, XPathConstants.NUMBER)).intValue();
            int compactInterval = ((Number)xPath.evaluate("snapshot/compact-interval", config, XPathConstants.NUMBER)).intValue();
            int stateChangeThreshold = ((Number)xPath.evaluate("snapshot/state-change-threshold", config, XPathConstants.NUMBER)).intValue();
            int dirtyLogTolerance = ((Number)xPath.evaluate("snapshot/dirty-log-tolerance", config, XPathConstants.NUMBER)).intValue();
            if (maintainInterval <= 0 || compactInterval <= 0 || stateChangeThreshold <= 0 || dirtyLogTolerance <= 0) {
                throw new IllegalArgumentException("snapshot");
            }

            int availCriticalPoint = ((Number)xPath.evaluate("metrics/avail-critical-point", config, XPathConstants.NUMBER)).intValue();
            int recoveryCoolDown = ((Number)xPath.evaluate("metrics/recovery-cool-down", config, XPathConstants.NUMBER)).intValue();
            if (availCriticalPoint < 0 || recoveryCoolDown < 0) {
                throw new IllegalArgumentException("metrics");
            }

            String logDir = (String) xPath.evaluate("storage/log", config, XPathConstants.STRING);
            String stateDir = (String) xPath.evaluate("storage/state", config, XPathConstants.STRING);
            String lockerDir = (String) xPath.evaluate("storage/locker", config, XPathConstants.STRING);
            String snapshotDir = (String) xPath.evaluate("storage/snapshot", config, XPathConstants.STRING);
            if (logDir == null || logDir.isEmpty()) {
                throw new IllegalArgumentException("storage/log");
            }
            if (stateDir == null || stateDir.isEmpty()) {
                throw new IllegalArgumentException("storage/state");
            }
            if (lockerDir == null || lockerDir.isEmpty()) {
                throw new IllegalArgumentException("storage/lock");
            }
            if (snapshotDir == null || snapshotDir.isEmpty()) {
                throw new IllegalArgumentException("storage/snapshot");
            }

            URI localURI = URI.create(local);
            List<URI> remoteURIs = Collections.unmodifiableList(remote.stream().map(URI::create).collect(Collectors.toList()));

            if (! "raft".equals(localURI.getScheme())) {
                throw new IllegalArgumentException("scheme");
            }

            return new RaftConfig(localURI, remoteURIs, preVote, tick, heartbeat, election, broadcast, logDir, stateDir, lockerDir, snapshotDir, triggerInterval, maintainInterval, compactInterval, stateChangeThreshold, dirtyLogTolerance, availCriticalPoint, recoveryCoolDown);
        }
    }

    public Path logPath() { return Paths.get(logPath); }

    public Path statePath() { return Paths.get(statePath); }

    public Path lockerPath() { return Paths.get(lockerPath); }

    public Path snapshotPath() { return Paths.get(snapshotPath); }

    public URI localURI() { return localURI; }

    public List<URI> remoteURIs() { return remoteURIs; }

    public boolean preVote() {
        return preVote;
    }

    public int electionTimeout() {
        int electTimeout = (int) Math.round(election * tick);
        return ThreadLocalRandom.current().nextInt(electTimeout, 2 * electTimeout + 1) ;
    }

    public int heartbeatInterval() {
        return (int) Math.round(heartbeat * tick);
    }

    public int broadcastTimeout() {
        return (int) Math.round(broadcast * tick);
    }

    public long snapTriggerInterval() {
        return TimeUnit.SECONDS.toMillis(triggerInterval);
    }

    public long snapMaintainInterval() {
        return TimeUnit.SECONDS.toMillis(maintainInterval);
    }

    public long snapCompactInterval() {
        return TimeUnit.SECONDS.toMillis(compactInterval);
    }

    public int snapStateChangeThreshold() {
        return stateChangeThreshold;
    }

    public int snapDirtyLogTolerance() {
        return dirtyLogTolerance;
    }

    public int availableCriticalPoint() {
        return availableCriticalPoint;
    }

    public int recoveryCoolDownMills() {
        return recoveryCoolDownMills;
    }
}
