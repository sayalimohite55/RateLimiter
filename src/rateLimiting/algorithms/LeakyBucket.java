package rateLimiting.algorithms;

import java.util.List;
import java.util.concurrent.*;

/*
* Basic Structure:
* A queue of fixed size that acts like a bucket
* A worker thread processes one request at a time at a fixed rate
* If queue is full, new requests are dropped
* */
public class LeakyBucket {
    private static class Request {
        private final int requestId;
        private final String userId; //Request metadata

        public Request(int requestId, String userId) {
            this.requestId = requestId;
            this.userId = userId;
        }

        public void process() {
            System.out.println("User: " + userId + " Processing " + requestId + " at " + System.currentTimeMillis());
        }
    }

    private final BlockingQueue<Request> bucket;
    private final ScheduledExecutorService worker;

    public LeakyBucket(int capacity, long leakIntervalMs) {
        this.bucket = new LinkedBlockingQueue<>(capacity);
        this.worker = Executors.newSingleThreadScheduledExecutor();

        // Leak at fixed interval (process one request at a time)
        worker.schedule(()-> {
                Request request = bucket.poll();
                if(request != null) {
                    request.process();
                }
        },leakIntervalMs, TimeUnit.MICROSECONDS);
    }

    public boolean allowRequest(Request req) {
        return bucket.offer(req); // false if full
    }

    public void shutdown() {
        worker.shutdown();
    }

    public static void main(String[] args) {
        LeakyBucket leakyBucket = new LeakyBucket(5,1000); //5 tokens capacity and interval per 1 sec

        //Simulate Requests
        for(int i=0;i<10; i++) {
            if(leakyBucket.allowRequest(new Request(i, "User"))) {
                System.out.println("Request : "+(i + 1)+" is allowed in token bucket");
            } else {
                System.out.println("Request " + (i + 1) + " denied");
            }

            try {
                Thread.sleep(200); //Simulate the time between the requests
            } catch(InterruptedException e) {
                e.printStackTrace();
            }

            leakyBucket.shutdown();
        }
    }
}
