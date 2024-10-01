package dadkvs.server.util;

public class ExponentialBackoff {
private long initialDelayMillis;
  private long maxDelayMillis;
  private long currentDelayMillis;

  public ExponentialBackoff(long initialDelayMillis, long maxDelayMillis) {
    this.initialDelayMillis = initialDelayMillis;
    this.maxDelayMillis = maxDelayMillis;
    this.currentDelayMillis = initialDelayMillis;
  }

  public void backoff() throws InterruptedException {
    Thread.sleep(currentDelayMillis);
    increaseDelay();
  }

  private void increaseDelay() {
    currentDelayMillis = Math.min(currentDelayMillis * 2, maxDelayMillis);
  }

  public void reset() {
    currentDelayMillis = initialDelayMillis;
  }
}
