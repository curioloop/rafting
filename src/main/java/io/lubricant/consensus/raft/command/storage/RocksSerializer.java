package io.lubricant.consensus.raft.command.storage;

import io.lubricant.consensus.raft.support.serial.CmdSerializer;
import io.lubricant.consensus.raft.command.RaftStub.Command;
import io.lubricant.consensus.raft.command.RaftLog.Entry;
import io.lubricant.consensus.raft.support.anomaly.SerializeException;

public class RocksSerializer implements CmdSerializer {

    @Override
    public Command deserialize(Entry entry) throws SerializeException {
        if (entry == null) return null;
        if (entry instanceof RocksEntry) {
            return RocksLog.entryToCmd((RocksEntry) entry);
        }
        throw new SerializeException("only support rocks entry");
    }

}
