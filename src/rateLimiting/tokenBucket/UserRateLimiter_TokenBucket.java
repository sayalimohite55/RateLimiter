package rateLimiting.tokenBucket;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/*
* Question:
* Requirements: Each user has:
*   A bucket of tokens
*   A maximum capacity
*   A refill rate (tokens per second)
* Implement a method:
*   boolean allowRequest(String userId) — returns true if the request is allowed for the given user.
*   Simulate multiple users making requests concurrently using Token Bucket.
*
* Follow up : Add a credit system to carry forward unused tokens
* */
public class UserRateLimiter_TokenBucket {
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
    private final ConcurrentHashMap<String, TokenBucket> userBuckets;

    public UserRateLimiter_TokenBucket() {
        userBuckets = new ConcurrentHashMap<>();
    }

    public void registerUser(String userId, int capacity, int refillRate, int carryForwardMultiplier) {
        userBuckets.computeIfAbsent(userId,a->new TokenBucket(capacity,refillRate, carryForwardMultiplier));
    }

    public boolean allowRequest(String userId) {
        TokenBucket bucket = userBuckets.getOrDefault(userId, null);
        return bucket != null && bucket.allowRequest();
    }

    public static void main(String[] args) {
        UserRateLimiter_TokenBucket rateLimiter = new UserRateLimiter_TokenBucket();
        rateLimiter.registerUser("userA",5,1, 2); //5 tokens, 1 token/sec
        rateLimiter.registerUser("userB",3,2, 2); //3 tokens, 2 tokens/sec

        ExecutorService executor = Executors.newFixedThreadPool(4);

        Runnable userAThread = () -> {
            for (int i=0; i<10; i++) {
                boolean allowedRequest = rateLimiter.allowRequest("userA");
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
                boolean allowedRequest = rateLimiter.allowRequest("userB");
                System.out.println("UserB requested requestId:"+i+" is allowed to request: "+allowedRequest);
                try {
                    Thread.sleep(400);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        executor.submit(userAThread);
        executor.submit(userBThread);
        executor.shutdown();
    }

}
