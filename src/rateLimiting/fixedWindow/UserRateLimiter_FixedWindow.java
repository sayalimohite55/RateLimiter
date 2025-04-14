package rateLimiting.fixedWindow;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class UserRateLimiter_FixedWindow {
    public static class Request{
        private final int requestId;
        private final String userId;

        public Request(int requestId, String userId) {
            this.requestId = requestId;
            this.userId = userId;
        }

        public void process(){
            System.out.println("Processing "+userId+" request: "+requestId+" at: "+System.currentTimeMillis());
        }
    }

    public static class Window {
        private long windowStartTime;
        private int requestCount;
        private int availableCredits;

        public Window(long windowStartTime) {
            this.windowStartTime = windowStartTime;
            this.requestCount = 0;
            this.availableCredits = 0;
        }

        public void reset(long windowStartTime, int maxRequests, int maxCreditLimit) {
            int unUsedCredits = maxRequests - this.requestCount;
            this.availableCredits = Math.min(maxCreditLimit, availableCredits + unUsedCredits);
            System.out.println("Newly updated available credit: "+ availableCredits);
            this.windowStartTime = windowStartTime;
            this.requestCount = 0;
        }
    }

    private final long windowSize;
    private final int maxRequests;
    private final int maxCreditLimit;
    private final ConcurrentHashMap<String,Window> userWindows;

    public UserRateLimiter_FixedWindow(long windowSize, int maxRequests, int maxCreditLimit) {
        this.windowSize = windowSize;
        this.maxRequests = maxRequests;
        this.maxCreditLimit = maxCreditLimit;
        this.userWindows = new ConcurrentHashMap<>();
    }

    public void registerUser(String userId) {
        this.userWindows.computeIfAbsent(userId,a->new Window(System.currentTimeMillis()));
    }

    public boolean allowRequest(Request request) {
        Window currentWindow = userWindows.get(request.userId);
        if(currentWindow != null) {
            long windowClosingTime = currentWindow.windowStartTime + windowSize;
            long currentTime = System.currentTimeMillis();

            synchronized (currentWindow) {
                if (currentTime > windowClosingTime) {
                    //Reset the window
                    currentWindow.reset(currentTime,maxRequests,maxCreditLimit);
                } else if (currentWindow.requestCount >= maxRequests ) {
                    if(currentWindow.availableCredits > 0) {
                        currentWindow.availableCredits--;
                        System.out.println("Availing credit for "+request.userId
                                +". Remaining credits are: "+currentWindow.availableCredits);
                    } else {
                        //No extra credits available
                        return false;
                    }
                }
                request.process();
                currentWindow.requestCount++;
                return true;
            }
        }
        System.out.println("Error: User-"+request.userId+" not registered. Hence, request failed");
        return false;
    }

    public static void main(String[] args) {
        UserRateLimiter_FixedWindow rateLimiter = new UserRateLimiter_FixedWindow(1000,10, 15);

        rateLimiter.registerUser("User A");
        rateLimiter.registerUser("User B");

        //Simulate user requests
        Runnable userA = () -> {
            for (int i=0;i<10; i++) {
                boolean allowed = rateLimiter.allowRequest(new Request(i,"User A"));
                System.out.println("User A requesting: "+i+" request -> status: "+allowed);

                try {
                    Thread.sleep(400); //Simulate delay between requests
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        Runnable userB = () -> {
            for (int i=0;i<10; i++) {
                boolean allowed = rateLimiter.allowRequest(new Request(i,"User B"));
                System.out.println("User B requesting: "+i+" request -> status: "+allowed);

                try {
                    Thread.sleep(600); //Simulate delay between requests
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.submit(userA);
        executorService.submit(userB);
        executorService.shutdown();
        try {
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    /*
    * Is it ok to drop all rejected requests?
    * -> Yes, in many cases, especially for public API's, dropping i.e rejecting requests with status code 429
    *    (Too many requests) is perfectly acceptible and efficient
    *
    * Common Strategies for Rejected Requests:
    * 1. Drop Immediately
    * -> Use this when you have a low latency requirement, the system needs to be protected against abuse and
    *    it's okay for users to retry later.
    *    Use case: Public API's, login endpoints
    *    What to do in such cases:
    *       - Log Rejections
    *       - Return 429 Too Many Requests with Retry-After header
    *
    * 2. Queue for Later (Server side Queueing)
    * -> Use this when you want to do eventual processing (eg. payments, orders).
    *    The system can afford a bit of latency or delay.
    *    This requires queue mechanism (RabbitMQ, Kafka, etc) and it increases complexity.
    *
    * 3. Client side Retry with Exponential Backoff
    * -> You return 429 to clients and expect them to retry .
    *    In such cases, combine rate-limiting with headers for clients to insist retries.
    *    e.g Retry-After, X-RateLimit-Reset
    *
    * 4. Dynamic Throttling with Priority Queues
    * -> Use this strategy when some requests need to be prioritised or you want fair usage among
    *    endpoints or clients.
    *
    * 5. Log and Alert on Excess Rejections
    * -> Even if you're rejecting a request, log and monitor request parameters:
    *       - Who is getting rate limited?
    *       - How Often?
    *       - Did something spike suddenly?
    *
    * */
}
