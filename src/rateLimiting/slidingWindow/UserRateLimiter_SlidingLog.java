package rateLimiting.slidingWindow;

import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class UserRateLimiter_SlidingLog {
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

    public static class SlidingLog {
        private final int maxRequests;
        private final long windowSize;
        private final Deque<Long> timeStampLog;
        private final int maxCredits;
        private int availableCredits;
        private long lastCreditRefillTime;
        private double creditBucket = 0.0;

        public SlidingLog(int maxRequests, long windowSize, int maxCredits) {
            this.maxRequests = maxRequests;
            this.windowSize = windowSize;
            this.timeStampLog = new LinkedList<>();
            this.maxCredits = maxCredits;
            this.availableCredits = 0;
            this.lastCreditRefillTime = System.currentTimeMillis();
        }

        public boolean allowRequest(Request request) {
            refillCredits();
            long now = System.currentTimeMillis();
            long windowStartTime = now - windowSize;

            synchronized (timeStampLog){
                //Remove all older entries
                while (!timeStampLog.isEmpty() && timeStampLog.peekFirst() < windowStartTime)
                    timeStampLog.pollFirst();

                if (timeStampLog.size() >= maxRequests) {
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

                timeStampLog.addLast(now);
                request.process();
                return true;
            }
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

    private final ConcurrentHashMap<String, SlidingLog> userLogMap;

    public UserRateLimiter_SlidingLog() {
        this.userLogMap = new ConcurrentHashMap<>();
    }

    public boolean allowRequest(Request request) {
        SlidingLog slidingLog = userLogMap.get(request.userId);
        if(slidingLog == null) {
            System.out.println("User - "+request.userId+" not registered. Request denied");
            return false;
        }
        return slidingLog.allowRequest(request);
    }

    public void registerUser(String userId, int maxRequests, long windowSize, int maxCredits) {
        userLogMap.computeIfAbsent(userId, a->new SlidingLog(maxRequests,windowSize, maxCredits));
    }

    public static void main(String[] args) {
        //Simulate multiple users scenario
        UserRateLimiter_SlidingLog rateLimter = new UserRateLimiter_SlidingLog();
        rateLimter.registerUser("User A", 5, 100, 7);
        rateLimter.registerUser("User B", 3, 200, 5);

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
