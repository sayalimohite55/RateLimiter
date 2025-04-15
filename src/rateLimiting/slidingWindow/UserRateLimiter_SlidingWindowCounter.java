package rateLimiting.slidingWindow;


import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class UserRateLimiter_SlidingWindowCounter {

    public static class Request{
        private final int requestId;
        private final String userId;

        public Request(int requestId, String userId) {
            this.requestId = requestId;
            this.userId = userId;
        }

        public void process() {
            System.out.println("Processing "+userId+" request:"+requestId+" at time: "+System.currentTimeMillis());
        }
    }

    public static class SlidingWindowCounter {
        private final int maxRequests;
        private final long windowSize;
        private final long bucketSize;
        private final ConcurrentSkipListMap<Long, AtomicInteger> timeStampLog;
        private final int maxCredits;
        private int availableCredits;
        private long lastCreditRefillTime;
        private double creditBucket = 0.0;

        public SlidingWindowCounter(int maxRequests, long windowSize, long bucketSize, int maxCredits) {
            this.maxRequests = maxRequests;
            this.windowSize = windowSize;
            this.bucketSize = bucketSize;
            this.timeStampLog = new ConcurrentSkipListMap<>();
            this.maxCredits = maxCredits;
            this.availableCredits = 0;
            this.lastCreditRefillTime = System.currentTimeMillis();
        }

        public boolean allowRequest(Request request) {
            refillCredits();
            long now = System.currentTimeMillis();
            long windowStartTime = now - windowSize;
            long currentBucket = now - (now % bucketSize);

            //Remove all entries before start time
            timeStampLog.headMap(windowStartTime).clear();

            int totalRequests = timeStampLog.tailMap(windowStartTime).values().stream()
                    .mapToInt(AtomicInteger::get).sum();

            if(totalRequests >= maxRequests) {
                if(availableCredits > 0) {
                    //Availing credits
                    this.availableCredits--;
                    System.out.println(request.userId+" availing extra credit for request: "+
                            request.requestId+". Remaining credits are:"+availableCredits);
                } else {
                    //No extra credits to avail
                    return false;
                }
            }

            timeStampLog.computeIfAbsent(currentBucket,a->new AtomicInteger(0)).incrementAndGet();
            request.process();
            return true;
        }

        private void refillCredits() {
            long now = System.currentTimeMillis();
            long timeSinceLastRefill = now - lastCreditRefillTime;

            double creditsPerMs = (double) maxCredits / windowSize;
            //Accumulated fractional credits.
            creditBucket += timeSinceLastRefill * creditsPerMs;
            int creditsToAdd = (int) creditBucket;
            if(creditsToAdd > 0) {
                availableCredits = Math.min(maxCredits, availableCredits + creditsToAdd);
                System.out.println("Refilled credits. Available Credits are: "+availableCredits);
                lastCreditRefillTime = now;
            }
        }
    }

    private final ConcurrentHashMap<String, SlidingWindowCounter> userMap;

    public UserRateLimiter_SlidingWindowCounter() {
        this.userMap = new ConcurrentHashMap<>();
    }

    public boolean allowRequest(Request request) {
        SlidingWindowCounter slidingWindow = userMap.get(request.userId);
        if(slidingWindow == null) {
            System.out.println("User - "+request.userId+" not registered. Request denied");
            return false;
        }
        return slidingWindow.allowRequest(request);
    }

    public void registerUser(String userId, int maxRequests, long windowSize, long bucketSize, int maxCredits) {
        userMap.computeIfAbsent(userId, a->new SlidingWindowCounter(maxRequests,windowSize, bucketSize, maxCredits));
    }

    public static void main(String[] args) {
        //Simulate multiple users scenario
        UserRateLimiter_SlidingWindowCounter rateLimter = new UserRateLimiter_SlidingWindowCounter();
        rateLimter.registerUser("User A", 5, 100, 10, 7);
        rateLimter.registerUser("User B", 3, 200, 20, 5);

        ExecutorService executorService = Executors.newFixedThreadPool(2);

        Runnable userA = () -> {
            for(int i =0;i<20; i++) {
                boolean allowed = rateLimter.allowRequest(new Request(i,"User A"));
                System.out.println("User A requested: "+i+" -> status: "+allowed);
            }
        };

        Runnable userB = () -> {
            for(int i =0;i<15; i++) {
                boolean allowed = rateLimter.allowRequest(new Request(i,"User B"));
                System.out.println("User B requested: "+i+" -> status: "+allowed);
            }
        };

        executorService.submit(userA);
        executorService.submit(userB);
        executorService.shutdown();
        try {
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
