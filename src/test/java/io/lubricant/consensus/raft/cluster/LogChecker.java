package io.lubricant.consensus.raft.cluster;

import io.lubricant.consensus.raft.cluster.cmd.AppendCommand;
import io.lubricant.consensus.raft.command.RaftLog;
import io.lubricant.consensus.raft.command.storage.RocksSerializer;
import io.lubricant.consensus.raft.command.storage.RocksStateLoader;
import io.lubricant.consensus.raft.support.RaftConfig;

public class LogChecker {

    public static void main(String[] args) throws Exception {
        RocksSerializer serializer = new RocksSerializer();
        RocksStateLoader load1 = new RocksStateLoader(RaftConfig.loadXmlConfig("raft1.xml", true));
        RocksStateLoader load2 = new RocksStateLoader(RaftConfig.loadXmlConfig("raft2.xml", true));
        RaftLog log1  = load1.restore("root", true);
        RaftLog log2  = load2.restore("root", true);
        System.out.println("epoch : " + log1.epoch().index() + " : " + log2.epoch().index());

        RaftLog.Entry last1 = log1.last();
        RaftLog.Entry last2 = log2.last();
        AppendCommand c1 = (AppendCommand) serializer.deserialize(last1);
        AppendCommand c2 = (AppendCommand) serializer.deserialize(last2);
        System.out.println(last1.index() + " : " + c1.line());
        System.out.println(last2.index() + " : " + c2.line());

        int index = 3050, length = 10;
        RaftLog.Entry[] batch1 = log1.batch(index, length);
        RaftLog.Entry[] batch2 = log2.batch(index, length);
        for (int i=0; i<length; i++) {
            AppendCommand cc1 = (AppendCommand) serializer.deserialize(batch1[i]);
            AppendCommand cc2 = (AppendCommand) serializer.deserialize(batch2[i]);
            System.out.println((i+index) + " : " + cc1.line() + "  " + cc2.line());
        }


    }
}
