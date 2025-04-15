package rateLimiting.algorithms;

import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/*
* Sliding Window:
* Sliding window offers smoother request distribution than fixed window by calculating rate over the moving time
* interval. There are two common approaches to implement sliding window:
* 1. Sliding Log (Timestamp log)
*    - Keeps a log of timestamps for requests and removes old ones
*
* 2. Sliding Window Counter
*    - keeps counters in small sub windows and sums them to estimate usage
*
* */
public class SlidingWindow {
    public static class Request {
        private final int requestId;
        private final String userId;

        public Request(int requestId, String userId) {
            this.requestId = requestId;
            this.userId = userId;
        }

        public void process() {
            System.out.println("Processing " + userId + " request: " + requestId + " at: " + System.currentTimeMillis());
        }
    }

    public static class SlidingLog {
        /*
         * Basic Structure:
         * In this approach, we maintain a timestamped log of requests and allow a request only if
         * the no. of requests in past window size (milliseconds) is below the limit
         *
         * Key - Components:
         * Window size eg. 1 minute
         * Max requests allowed in that window.
         * Queue like structure to maintain log of timestamps
         * On each request:
         *   - Remove outdated timestamps
         *   - if size of queue (i.e remaining timestamps) < maxRequests -> allow request and log timestamp
         *   - otherwise -> reject request
         * */
        private int maxRequests;
        private long windowSizeMs;
        private final Deque<Long> logQueue;

        public SlidingLog(int maxRequests, long windowSizeMs) {
            this.maxRequests = maxRequests;
            this.windowSizeMs = windowSizeMs;
            this.logQueue = new LinkedList<>();
        }

        public  boolean allowRequest(Request request) {
            long now = System.currentTimeMillis();
            synchronized (logQueue) {
                while(!logQueue.isEmpty() && logQueue.peekFirst() <= now-windowSizeMs) //Remove older logs
                    logQueue.pollFirst();

                if(logQueue.size() < maxRequests) {
                    logQueue.addLast(now);
                    request.process();
                    return true;
                }
                return false;
            }
        }
    }

    public static class SlidingWindowCounter {
        /*
        * Idea: Divide the main window (eg. 1 minute) into smaller sub-windows (eg. 10 second buckets
        * You maintain a count of requests in each sub window and slide the window forward by clearing
        * out old buckets.
        *
        * Key Components:
        * windowSize -> Total Sliding window duration(eg. 60 seconds)
        * bucketSize -> Granularity of buckets (eg. 10)
        * bucketCount = windowSize / bucketSize
        * buckets -> a map of timestamp -> count
        * On each request->
        *   - remove / ignore expired buckets
        *   - sum counts of all,valid buckets
        *   - if sum under limit -> allow request and update current bucket
        *   - else reject
        * */
        private final long windowSize;
        private final long bucketSize;
        private final int maxRequests;
        private final ConcurrentSkipListMap<Long, AtomicInteger> bucketMap;

        public SlidingWindowCounter(long windowSize,long bucketSize, int maxRequests) {
            this.windowSize = windowSize;
            this.bucketSize = bucketSize;
            this.maxRequests = maxRequests;
            this.bucketMap = new ConcurrentSkipListMap<>();
        }

        public boolean allowRequest(Request request) {
            long now = System.currentTimeMillis();
            long windowStart = now - windowSize;

            //clear or remove entries older than windowStart time
            bucketMap.headMap(windowStart).clear();

            int totalRequests = bucketMap.tailMap(windowStart).values().stream()
                    .mapToInt(AtomicInteger::get).sum();

            if(totalRequests > maxRequests)
                return false;

            bucketMap.computeIfAbsent(getBucketKey(),a->new AtomicInteger(0)).incrementAndGet();
            request.process();
            return true;
        }

        private long getBucketKey() {
            long now = System.currentTimeMillis();
            return now - (now % bucketSize);
        }

    }

    public static void main(String[] args) {

        //Simulate requests for SlidingLog
//        SlidingLog rateLimiter = new SlidingLog(5, 3000);
//
//        Runnable simulateUser = () -> {
//            String userId = "User1";
//            for (int i = 0; i < 10; i++) {
//                boolean allowed = rateLimiter.allowRequest(new Request(i, userId));
//                System.out.println("User1 requesting: "+i+"-> status: "+allowed);
//                try {
//                    Thread.sleep(400);
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//        };
//
//        ExecutorService executor = Executors.newSingleThreadExecutor();
//        executor.submit(simulateUser);
//        executor.shutdown();

        //simulate requests for SlidingCounter
        SlidingWindowCounter slidingWindow_rateLimiter = new SlidingWindowCounter(1000, 30, 5);

        System.out.println();
        Runnable simulateUser = () -> {
            String userId = "User1";
            for (int i = 0; i < 10; i++) {
                boolean allowed = slidingWindow_rateLimiter.allowRequest(new Request(i, userId));
                System.out.println("User1 requesting: "+i+"-> status: "+allowed);
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(simulateUser);
        executor.shutdown();
    }
}
