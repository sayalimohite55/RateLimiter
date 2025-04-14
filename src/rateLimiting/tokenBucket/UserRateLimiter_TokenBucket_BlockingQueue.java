package rateLimiting.tokenBucket;

import java.util.concurrent.*;

/*
 * Question:
 * Requirements: Each user has:
 *   A bucket of tokens
 *   A maximum capacity
 *   A refill rate (tokens per second)
 * Implement a method:
 *   boolean allowRequest(String userId) — returns true if the request is allowed for the given user.
 *   Simulate multiple users making requests concurrently.
 *
 * Follow up : Add a credit system to carry forward unused tokens
 * */
public class UserRateLimiter_TokenBucket_BlockingQueue {
    /*
     * Token Bucket algorithm works as follows:
     * User wise buckets are created and each bucket has a capacity to hold tokens
     * When a request comes, if token is present in user's bucket, request will be allowed
     * If bucket is empty, request is not allowed, so it will have to wait for token to get added into bucket
     * When a request is allowed token count is decremented by one.
     * We have defined a rate of refilling i.e frequency of adding tokens to the bucket.
     *
     * We have also implemented credit system such that if a user does not use all request attempts it will
     * be carried forward. To make it efficient we have put a limit on how many tokens can be credited using
     * a max multiplier for capacity.
     * */

    /*
     * What happens to denied requests in Token Bucket?
     * -> In a standard implementation, if there are no tokens available, the request is rejected immediately
     * -> As there is no queueing, no retry. In such cases, its clients responsibility to backoff or retry.
     *
     * Optional Strategies to handle rejected requests:
     * -> 1. Client side retry using exponential backoff
     *       eg.
     *       while (!rateLimiter.allowRequest(userId)) {
     *           Thread.sleep(200); // Wait before retrying
     *       }
     *       Well this is simple, but can increase load, if too many clients retry too soon.
     *
     * -> 2. Server-side queue (Buffer until tokens are available)
     *       Queue the request and process it later when tokens are refilled.
     *       This requires a request queue(Blocking Queue) and a worker thread can pull all requests only if a
     *       token is available.
     *       This makes TokenBucket more like a Producer-Consumer.
     *       It adds some complexity and extra memory but smoothens the spikes
     *
     * -> 3. Drop requests with logging and alerting silently
     * */

    static class TokenBucket {
        private final int capacity;
        private final int rate;
        private final int maxCarryForwardMultiplier;
        private int tokens;
        private long lastFillTime;

        public TokenBucket(int capacity, int rate, int maxCarryForwardMultiplier) {
            this.capacity = capacity;
            this.rate = rate;
            this.maxCarryForwardMultiplier = maxCarryForwardMultiplier;
            this.tokens = capacity;
            this.lastFillTime = System.nanoTime();
        }

        synchronized boolean allowRequest() {
            this.refill();
            if(this.tokens > 0){
                this.tokens --;
                return true;
            }
            return false;
        }

        private void refill() {
            long currentTime = System.nanoTime();
            long passedTime = currentTime - this.lastFillTime;
            int tokensToAdd = (int) (passedTime/ TimeUnit.SECONDS.toNanos(1)) * this.rate;
            if(tokensToAdd > 0) {
                int maxTokens = maxCarryForwardMultiplier * capacity;
                this.tokens = Math.min(maxTokens, this.tokens + tokensToAdd); //Credit system
                lastFillTime = currentTime;
            }
        }
    }

    private static class Request{
        private final int requestId;
        private final String userId; //Request metadata

        public Request(int requestId, String userId) {
            this.requestId = requestId;
            this.userId = userId;
        }

        public void process() {
            System.out.println("User: "+userId+" Processing " + requestId + " at " + System.currentTimeMillis());
        }
    }

    private final ConcurrentHashMap<String, TokenBucket> userBuckets;
    private final BlockingQueue<Request> requestQueue;
    private final ExecutorService consumerExecutor;

    public UserRateLimiter_TokenBucket_BlockingQueue() {
        userBuckets = new ConcurrentHashMap<>();
        requestQueue = new LinkedBlockingQueue<>(100);
        consumerExecutor = Executors.newSingleThreadExecutor();
        startConsumer();
    }

    public void startConsumer() {
        consumerExecutor.submit(()-> {
            //Previous Approach
//            while(true) {
//                if(!requestQueue.isEmpty()) {
//                    Request request = requestQueue.peek();
//                    TokenBucket bucket = userBuckets.getOrDefault(request.userId, null);
//                    if(bucket.allowRequest()) {
//                        request.process();
//                        requestQueue.remove();
//                    }
//                }
//            try {
//                Thread.sleep(200);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
//            }
            //Better Approach:
            Request request = null; // Blocks until available
            try {
                request = requestQueue.take();
                TokenBucket bucket = userBuckets.get(request.userId);
                if (bucket.allowRequest()) {
                    request.process();
                } else {
                    requestQueue.offer(request); // Requeue if still not allowed
                    Thread.sleep(100); // Wait briefly before retrying
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void registerUser(String userId, int capacity, int refillRate, int carryForwardMultiplier) {
        userBuckets.computeIfAbsent(userId,a->new TokenBucket(capacity,refillRate, carryForwardMultiplier));
    }

    public boolean allowRequest(Request request) {
        TokenBucket bucket = userBuckets.getOrDefault(request.userId, null);
        if(bucket == null) {
            return false;
        } else if(bucket.allowRequest()) {
            return true;
        } else {
            //add to queue and return true
            requestQueue.add(request);
            return true;
        }
    }

    public void shutdown() {
        consumerExecutor.shutdown();
    }

    public static void main(String[] args) {
        UserRateLimiter_TokenBucket_BlockingQueue rateLimiter = new UserRateLimiter_TokenBucket_BlockingQueue();
        rateLimiter.registerUser("userA",5,1, 1); //5 tokens, 1 token/sec
        rateLimiter.registerUser("userB",3,2, 1); //3 tokens, 2 tokens/sec

        ExecutorService producerExecutor = Executors.newFixedThreadPool(4);

        Runnable userAThread = () -> {
            for (int i=0; i<10; i++) {
                boolean allowedRequest = rateLimiter.allowRequest(new Request(i,"userA"));
                System.out.println("UserA requested requestId:"+i+" is allowed to request: "+allowedRequest);
                try {
                    Thread.sleep(300);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        Runnable userBThread = () -> {
            for (int i=0; i<10; i++) {
                boolean allowedRequest = rateLimiter.allowRequest(new Request(i,"userB"));
                System.out.println("UserB requested requestId:"+i+" is allowed to request: "+allowedRequest);
                try {
                    Thread.sleep(400);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        };


        producerExecutor.submit(userAThread);
        producerExecutor.submit(userBThread);
        producerExecutor.shutdown();

        try {
            producerExecutor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        rateLimiter.shutdown();
    }
    /*
    * Set of enhancements that can be done:
    * -> Timeouts for queued request (Consider a timestamp in request and drop if it's been waiting for too long
    *    if (System.currentTimeMillis() - request.timestamp > 5000) { //5 seconds
    *       logDroppedRequest();
    *    }
    *
    * -> Graceful Shoutdown
    *    Stop the consumer thread cleanly with a flag or consumerExecutor.shutdownNow() and
    *    catching InterruptedException
    * */
}
