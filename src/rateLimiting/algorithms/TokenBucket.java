package rateLimiting.algorithms;

import java.util.concurrent.TimeUnit;

public class TokenBucket {
    private final int capacity;
    private final int rate;
    private int tokens;
    private long lastRefillTime;

    public TokenBucket(int capacity, int rate) {
        this.capacity = capacity;
        this.rate = rate;
        this.tokens = capacity;
        this.lastRefillTime = System.nanoTime();
    }

    public synchronized boolean allowRequest() {
        refill();
        if(this.tokens > 0) {
            tokens--;
            return true;
        }
        return false;
    }

    private void refill() {
        long currentTime = System.nanoTime();
        long timePassed = currentTime - lastRefillTime;

        int tokensToAdd = (int) (timePassed/ TimeUnit.SECONDS.toNanos(1)) * rate;
        if(tokensToAdd > 0) {
            tokens = Math.min(capacity, tokens + tokensToAdd);
            lastRefillTime = currentTime;
        }
    }

    public static void main(String[] args) {
        TokenBucket tokenBucket = new TokenBucket(5,1); //5 tokens capacity and 1 token per second

        //Simulate Requests
        for(int i=0;i<10; i++) {
            if(tokenBucket.allowRequest()) {
                System.out.println("Request : "+(i + 1)+" is allowed in token bucket");
            } else {
                System.out.println("Request " + (i + 1) + " denied");
            }

            try {
                Thread.sleep(200); //Simulate the time between the requests
            } catch(InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
