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
import java.util.stream.Collectors;

/**
 * 配置
 */
public class RaftConfig {

    private final URI localURI;
    private final List<URI> remoteURIs;
    private final int tick;
    private final double heartbeat, election, broadcast;
    private final String logPath;
    private final String statePath;
    private final String lockerPath;
    private final String snapshotPath;

    public RaftConfig(String configPath, boolean classpath) throws Exception {

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

            Number tickNum = (Number) xPath.evaluate("count(timeout/tick)", config, XPathConstants.NUMBER);
            Number heartbeatNum = (Number) xPath.evaluate("count(timeout/heartbeat)", config, XPathConstants.NUMBER);
            Number electionNum = (Number) xPath.evaluate("count(timeout/election)", config, XPathConstants.NUMBER);
            Number broadcastNum = (Number) xPath.evaluate("count(timeout/broadcast)", config, XPathConstants.NUMBER);
            if (tickNum.intValue() != 1 || heartbeatNum.intValue() != 1 || electionNum.intValue() != 1 || broadcastNum.intValue() != 1) {
                throw new IllegalArgumentException("schedule");
            }

            int tick = ((Number)xPath.evaluate("timeout/tick", config, XPathConstants.NUMBER)).intValue();
            double heartbeat = ((Number)xPath.evaluate("timeout/heartbeat", config, XPathConstants.NUMBER)).doubleValue();
            double election = ((Number)xPath.evaluate("timeout/election", config, XPathConstants.NUMBER)).doubleValue();
            double broadcast = ((Number)xPath.evaluate("timeout/broadcast", config, XPathConstants.NUMBER)).doubleValue();
            if (tick <= 0 || heartbeat <= 0 || election <= 0 || broadcast <= 0) {
                throw new IllegalArgumentException("schedule");
            }
            if (heartbeat >= election || broadcast >= heartbeat) {
                throw new IllegalArgumentException("schedule");
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

            this.localURI = URI.create(local);
            this.remoteURIs = Collections.unmodifiableList(remote.stream().map(URI::create).collect(Collectors.toList()));

            if (! "raft".equals(localURI.getScheme())) {
                throw new IllegalArgumentException("scheme");
            }

            this.tick = tick;
            this.heartbeat = heartbeat;
            this.election = election;
            this.broadcast = broadcast;

            this.logPath = logDir + (logDir.endsWith(File.separator) ? "": File.separator);
            this.statePath = stateDir + (stateDir.endsWith(File.separator) ? "": File.separator);
            this.lockerPath = lockerDir + (lockerDir.endsWith(File.separator) ? "": File.separator);
            this.snapshotPath = snapshotDir + (lockerDir.endsWith(File.separator) ? "": File.separator);
        }
    }

    public Path logPath() { return Paths.get(logPath); }

    public Path statePath() { return Paths.get(statePath); }

    public Path lockerPath() { return Paths.get(lockerPath); }

    public Path snapshotPath() { return Paths.get(snapshotPath); }

    public URI localURI() { return localURI; }

    public List<URI> remoteURIs() { return remoteURIs; }

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
}
