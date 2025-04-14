package rateLimiting.algorithms;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/*
* Basic Structure:
* A window has a start time and count of requests in that window
* Fixed window has maxWindowSize and maxRequests that can be processed in a window
*
* */
public class FixedWindow {
    public static class Request {
        private final int requestId;
        private final String userId;

        public Request(int requestId, String userId) {
            this.requestId = requestId;
            this.userId = userId;
        }

        public void process() {
            System.out.println("Processing "+this.userId+" request: "+
                    this.requestId+" at time "+System.currentTimeMillis());
        }
    }

    public static class Window {
        private long windowStartTime;
        private int requestCount;

        public Window(long windowStartTime) {
            this.windowStartTime = windowStartTime;
            this.requestCount = 0;
        }

        public void reset(long newStartTime) {
            this.windowStartTime = newStartTime;
            this.requestCount = 0;
        }
    }

    private final int maxRequests;
    private final long windowSize;
    private final ScheduledExecutorService worker;
    private Window currentWindow;

    public FixedWindow(int maxRequests, long windowSize) {
        this.maxRequests = maxRequests;
        this.windowSize = windowSize;
        this.worker = Executors.newSingleThreadScheduledExecutor();
        this.currentWindow = new Window(System.currentTimeMillis());

        worker.scheduleAtFixedRate(()->{
            synchronized(currentWindow) {
                this.currentWindow.reset(System.currentTimeMillis());
            }
        },0,windowSize, TimeUnit.MILLISECONDS);
    }

    public boolean allowRequest(Request request) {
        long startTime = currentWindow.windowStartTime;
        long currentTime = System.currentTimeMillis();
        long windowClosingTime = startTime + windowSize;

        int requestCount = currentWindow.requestCount;
        if(currentTime < windowClosingTime && requestCount < maxRequests) {
            currentWindow.requestCount++;
            request.process();
            return true;
        }
        return false;
    }

    public void shutdown() {
        worker.shutdown();
    }

    public static void main(String[] args) {
        FixedWindow fixedWindow = new FixedWindow(5,5000); //5 requests and window size 5 seconds

        //Simulate Requests
        for(int i=0;i<10; i++) {
            if(fixedWindow.allowRequest(new Request(i, "User"))) {
                System.out.println("Request : "+(i + 1)+" is allowed in Fixed Window");
            } else {
                System.out.println("Request " + (i + 1) + " is denied");
            }

            try {
                Thread.sleep(200); //Simulate the time between the requests
            } catch(InterruptedException e) {
                e.printStackTrace();
            }

            fixedWindow.shutdown();
        }
    }

}
