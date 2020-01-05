package io.lubricant.consensus.raft.support.serial;


import io.lubricant.consensus.raft.command.RaftClient.Command;
import io.lubricant.consensus.raft.command.RaftLog.Entry;

/**
 * 命令序列化器
 */
public interface CmdSerializer {

    /**
     * 将日志转换为命令，供状态机执行
     */
    Command deserialize(Entry entry) throws SerializeException;

    default byte[] serialize(Command cmd) throws SerializeException {
        return Serialization.writeObject(cmd);
    }

    default byte[] serialize(byte[] meta, Command cmd) throws SerializeException {
        return Serialization.writeObject(meta, cmd);
    }

}
