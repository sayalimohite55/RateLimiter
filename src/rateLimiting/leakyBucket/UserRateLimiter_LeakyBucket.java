package rateLimiting.leakyBucket;

import rateLimiting.tokenBucket.UserRateLimiter_TokenBucket;

import java.util.concurrent.*;

/*
 * Question:
 * Requirements: Each user has:
 *   A bucket of requests
 *   rate of request processing
 * Implement a method:
 *   boolean allowRequest(String userId) — returns true if the request is allowed for the given user.
 *   Simulate multiple users making requests concurrently using Leaky Bucket.
 *
 * Follow up : Add a credit system to carry forward unused request attempts
 * If a user doesn't use all the allowed requests in one interval, they should be able to use
 * those credits later (within limits)
 * */
public class UserRateLimiter_LeakyBucket {
    /*
    * Intent:
    * Think of a bucket leaking water at a constant rate
    * Incoming requests are like water added to this bucket
    * If the bucket overflows (due to too many requests too fast), the request is rejected
    * Requests are processed at a fixed rate - smoothing bursts which enforces a steady outflow of requests
    *
    * There can be two ways to implement this:
    * 1. Global Leaky Bucket
    *    Use this when you want to put a overall limit on how much load a system/ endpoint should handle
    *    regardless of who is making requests.
    * -> Pros: It is simple to implement and protects shared backend resources from overload
    * -> Cons: One noisy user can starve other users. This does not provide fairness or per-user control.
    *
    * 2. Per-User Leaky Buckets
    *    Use this approach when you want fairness across users.
    * -> Pros: It isolates users such that, one user cannot affect others. It also allows personalised rate limits.
    * -> Cons: Slightly more complex to implement and has more memory consumption.
    *
    * Recommendation:
    * In real world systems, the best approach is usually hybrid.
    * -> Use per-user buckets to ensure isolation and fairness
    * -> Use global bucket or circuit breaker to cap total traffic and protect backend services
    * */
    static class Request{
        private final int requestId;
        private final String userId;

        public Request(int requestId, String userId) {
            this.requestId = requestId;
            this.userId = userId;
        }

        public void process(){
            System.out.println("Processing request: "+requestId+" of user: "+userId
                    +" using Leaky Bucket at: "+System.currentTimeMillis());
        }
    }

    static class LeakyBucket{
        private final BlockingQueue<Request> bucket;
        private final ScheduledExecutorService worker;
        private final int maxCapacityMultiplier = 2;
        private final int maxCapacity;
        private final int maxCredits;
        private int availableCredits = 0;


        public LeakyBucket(int capacity, long leakIntervalMs) {
            this.bucket = new LinkedBlockingQueue<>(capacity);
            this.worker = Executors.newSingleThreadScheduledExecutor();
            this.maxCapacity = capacity;
            this.maxCredits = (maxCapacityMultiplier - 1) * maxCapacity; // Carry forward allowed
            this.availableCredits = 0;

            //This will be invoked at a fixed interval of leakIntervalsMs
            worker.scheduleAtFixedRate(()-> {
                Request request = bucket.poll();
                if(request != null) {
                    request.process();
                } else if(availableCredits < maxCredits){
                    //No request to process, so save the slot as a credit
                    this.availableCredits++;
                    System.out.println("Credit earned. Available credits: " + availableCredits);
                }
            }, 0,leakIntervalMs, TimeUnit.MILLISECONDS);
        }

        public boolean allowRequest(Request request) {
            boolean offered = bucket.offer(request); // Returns false if bucket is full (in that case request will be dropped)
            if(!offered && availableCredits > 0) {
                // If bucket is full but we have credits, simulate capacity using credit
                availableCredits--;
                System.out.println("Using credit for "+request.userId+". Remaining credits: " + availableCredits);
                // Still process it by pushing it directly into a background thread
                CompletableFuture.runAsync(request::process); // optional: queue it if needed
                return true;
            }
            return offered;
        }

        public void shutdown() {
            worker.shutdown();
        }
    }

    private final ConcurrentHashMap<String,LeakyBucket> userBuckets = new ConcurrentHashMap<>();

    public void registerUser(String userId, int capacity, long leakIntervalsMs) {
        userBuckets.computeIfAbsent(userId,a->new LeakyBucket(capacity,leakIntervalsMs));
    }

    public boolean allowRequest(Request request) {
        LeakyBucket bucket = userBuckets.get(request.userId);
        if(bucket == null)
            return false;
        return bucket.allowRequest(request);
    }

    public void shutdown() {
        userBuckets.values().forEach(LeakyBucket::shutdown);
    }

    public static void main(String[] args) {
        //simulate real use-case scenario
        UserRateLimiter_LeakyBucket rateLimiter = new UserRateLimiter_LeakyBucket();

        rateLimiter.registerUser("UserA",5,100); //capacity - 5 , leak interval 1/sec
        rateLimiter.registerUser("UserB", 3, 50); //capacity - 3, leak interval 2/sec

        ExecutorService producers = Executors.newFixedThreadPool(2);

        Runnable userA = () -> {
            for(int i = 0;i<10; i++) {
                boolean allowedRequest = rateLimiter.allowRequest(new Request(i,"UserA"));
                System.out.println("UserA requested "+i+" allowed: "+allowedRequest);
                try {
                    Thread.sleep(30);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        Runnable userB = () -> {
            for(int i = 0;i<10; i++) {
                boolean allowedRequest = rateLimiter.allowRequest(new Request(i,"UserB"));
                System.out.println("UserB requested "+i+" allowed: "+allowedRequest);
                try {
                    Thread.sleep(300);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        producers.submit(userA);
        producers.submit(userB);
        producers.shutdown();
        try {
            producers.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        rateLimiter.shutdown();
    }

    /*
    * Set of enhancements that can be done:
    * Add a global rate limiting bucket as well
    * */
}
