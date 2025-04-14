package rateLimiting.leakyBucket;

import java.util.concurrent.*;

public class HybridRateLimiter_LeakyBucket {

    static class Request {
        private final int requestId;
        private final String userId;

        public Request(int requestId, String userId) {
            this.requestId = requestId;
            this.userId = userId;
        }

        public void process() {
            System.out.println("Processing request: " + requestId + " from user: " + userId + " at " + System.currentTimeMillis());
        }
    }

    static class LeakyBucket {
        private final BlockingQueue<Request> bucket;
        private final ScheduledExecutorService worker;
        private final int maxCapacityMultiplier = 2;
        private final int capacity;
        private final int maxCredits;
        private int availableCredits;

        public LeakyBucket(int capacity, long leakIntervalsMs) {
            this.capacity = capacity;
            this.maxCredits = this.capacity * maxCapacityMultiplier;
            this.availableCredits = 0;

            this.bucket = new LinkedBlockingQueue<>(this.capacity);
            this.worker = Executors.newSingleThreadScheduledExecutor();

            //This will be invoked at a fixed interval of leakIntervalsMs
            worker.scheduleAtFixedRate(()-> {
                Request request = this.bucket.poll();
                if(request != null) {
                    request.process();
                } else if( availableCredits < this.maxCredits){
                    this.availableCredits++;
                    System.out.println("Earned new credit. Remaining Credits are: "+availableCredits);
                }
            }, 0, leakIntervalsMs, TimeUnit.MILLISECONDS);
        }

        public boolean allowRequest(Request request) {
            boolean offered = bucket.offer(request);
            if(!offered && this.availableCredits > 0) {
                //If bucket is full but we have credits.
                this.availableCredits--;
                System.out.println("Using credit for "+request.userId+". Remaining credits are : "+availableCredits);
                //Still processing request by directly pushing it into background thread
                CompletableFuture.runAsync(request::process);
                return true;
            }
            return offered;
        }

        public void shutdown() {
            worker.shutdown();
        }
    }

    private ConcurrentHashMap<String, LeakyBucket> userBuckets;
    private LeakyBucket globalBucket;

    public HybridRateLimiter_LeakyBucket(int globalCapacity, long globalLeakIntervalMs) {
        this.userBuckets = new ConcurrentHashMap<>();
        this.globalBucket = new LeakyBucket(globalCapacity, globalLeakIntervalMs);
    }

    public void registerUser(String userId, int capacity, long leakyIntervalMs) {
        this.userBuckets.computeIfAbsent(userId,a->new LeakyBucket(capacity,leakyIntervalMs));
    }

    public boolean allowRequest(Request request) {
//        LeakyBucket bucket = userBuckets.get(request.userId);
//        return (bucket != null && bucket.allowRequest(request) && globalLeakyBucket.allowRequest(request));
        LeakyBucket userBucket = userBuckets.get(request.userId);
        if (userBucket == null) {
            System.out.println("[REJECTED] No user bucket found for user: " + request.userId);
            return false;
        }

        boolean globalAllowed = globalBucket.allowRequest(request);
        boolean userAllowed = userBucket.allowRequest(request);

        if (!globalAllowed) {
            System.out.println("[REJECTED][GLOBAL] Global bucket full for requestId: " + request.requestId +
                    " user: " + request.userId + " at " + System.currentTimeMillis());
        }

        if (!userAllowed) {
            System.out.println("[REJECTED][USER] User bucket full for requestId: " + request.requestId +
                    " user: " + request.userId + " at " + System.currentTimeMillis());
        }
        return globalAllowed && userAllowed;
    }

    public void shutdown() {
        globalBucket.shutdown();
        userBuckets.values().forEach(LeakyBucket::shutdown);
    }

    public static void main(String[] args) {
        HybridRateLimiter_LeakyBucket rateLimiter = new HybridRateLimiter_LeakyBucket(10,100);
        rateLimiter.registerUser("UserA",5,100);
        rateLimiter.registerUser("UserB",3,50);

        ExecutorService producer = Executors.newFixedThreadPool(2);

        Runnable userA = () -> {
            for(int i =0;i<10; i++) {
                boolean allowed = rateLimiter.allowRequest(new Request(i, "UserA"));
                System.out.println("UserA attempting to request: "+i+ " allowed: "+ allowed);
                try {
                    Thread.sleep(30);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        Runnable userB = () -> {
            for(int i =0;i<10; i++) {
                boolean allowed = rateLimiter.allowRequest(new Request(i, "UserB"));
                System.out.println("UserB attempting to request: "+i+ " allowed: "+ allowed);
                try {
                    Thread.sleep(300);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        producer.submit(userA);
        producer.submit(userB);
        producer.shutdown();
        try {
            producer.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        rateLimiter.shutdown();
    }

    /*
    * Notes:
    * Global buckets act as a shared limiter: if it overflows, all users get throttled
    * You can tweak leak intervals and capacities to balance between fairness and system protection
    * */
}
