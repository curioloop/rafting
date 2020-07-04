package io.lubricant.consensus.raft.transport;

import io.lubricant.consensus.raft.transport.rpc.Async;

import java.util.concurrent.*;

import static io.lubricant.consensus.raft.transport.rpc.Async.head;

public class AsyncFutureTest {

    public static void main(String[] args) throws InterruptedException {
        ScheduledExecutorService service = Executors.newScheduledThreadPool(1);

        Executor e = new Executor() {
            int i = 1;
            @Override
            public void execute(Runnable command) {
                service.schedule(command, (i++), TimeUnit.SECONDS);
            }
        };

        Async<String> call = Async.call(e, () -> null, null);

        Async.AsyncHead head = head();
        for (int i=0; i<5; i++) {
            call.on(head, 10000, new Async.AsyncCallback<String>() {
                @Override
                public void on(String result, Throwable error, boolean canceled) {
                    System.out.println("done " + (error == null ? "": error.getClass().getSimpleName()));
                }
            });
        }

        int i = 3;
        while (i <= 5 && ! head.isAborted()) {
            boolean b = head.waitRequests(i);
            System.out.println("finish " + b + " " + i++);
        }
    }
}
