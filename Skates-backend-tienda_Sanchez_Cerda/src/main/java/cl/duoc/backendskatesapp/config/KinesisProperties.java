package cl.duoc.backendskatesapp.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.kinesis")
public class KinesisProperties {

    private boolean enabled;
    private String region = "us-east-1";
    private String endpointOverride = "";
    private final Stream orders = new Stream("skates-orders-events", 3, 24);
    private final Stream inventory = new Stream("skates-inventory-events", 3, 168);
    private final Stream deadLetter = new Stream("skates-dead-letter-stream", 1, 168);
    private final Consumers consumers = new Consumers();
    private final Alarm alarm = new Alarm();

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public String getRegion() { return region; }
    public void setRegion(String region) { this.region = region; }
    public String getEndpointOverride() { return endpointOverride; }
    public void setEndpointOverride(String endpointOverride) { this.endpointOverride = endpointOverride; }
    public Stream getOrders() { return orders; }
    public Stream getInventory() { return inventory; }
    public Stream getDeadLetter() { return deadLetter; }
    public Consumers getConsumers() { return consumers; }
    public Alarm getAlarm() { return alarm; }

    public static class Stream {
        private String name;
        private int shardCount;
        private int retentionHours;
        private String capacityMode = "PROVISIONED";

        public Stream() { }
        public Stream(String name, int shardCount, int retentionHours) {
            this.name = name;
            this.shardCount = shardCount;
            this.retentionHours = retentionHours;
        }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public int getShardCount() { return shardCount; }
        public void setShardCount(int shardCount) { this.shardCount = shardCount; }
        public int getRetentionHours() { return retentionHours; }
        public void setRetentionHours(int retentionHours) { this.retentionHours = retentionHours; }
        public String getCapacityMode() { return capacityMode; }
        public void setCapacityMode(String capacityMode) { this.capacityMode = capacityMode; }
    }

    public static class Consumers {
        private String analyticsAppName = "skates-analytics-app";
        private String notificationsAppName = "skates-notifications-app";
        private String initialPosition = "LATEST";
        private int maxRetries = 5;
        private long backoffInitialMs = 500;
        private double backoffMultiplier = 2.0;
        private long maxBackoffMs = 10000;
        private int pollingMaxRecords = 100;
        private long failoverTimeMillis = 10000;
        private String appSelector = "all";

        public String getAnalyticsAppName() { return analyticsAppName; }
        public void setAnalyticsAppName(String analyticsAppName) { this.analyticsAppName = analyticsAppName; }
        public String getNotificationsAppName() { return notificationsAppName; }
        public void setNotificationsAppName(String notificationsAppName) { this.notificationsAppName = notificationsAppName; }
        public String getInitialPosition() { return initialPosition; }
        public void setInitialPosition(String initialPosition) { this.initialPosition = initialPosition; }
        public int getMaxRetries() { return maxRetries; }
        public void setMaxRetries(int maxRetries) { this.maxRetries = maxRetries; }
        public long getBackoffInitialMs() { return backoffInitialMs; }
        public void setBackoffInitialMs(long backoffInitialMs) { this.backoffInitialMs = backoffInitialMs; }
        public double getBackoffMultiplier() { return backoffMultiplier; }
        public void setBackoffMultiplier(double backoffMultiplier) { this.backoffMultiplier = backoffMultiplier; }
        public long getMaxBackoffMs() { return maxBackoffMs; }
        public void setMaxBackoffMs(long maxBackoffMs) { this.maxBackoffMs = maxBackoffMs; }
        public int getPollingMaxRecords() { return pollingMaxRecords; }
        public void setPollingMaxRecords(int pollingMaxRecords) { this.pollingMaxRecords = pollingMaxRecords; }
        public long getFailoverTimeMillis() { return failoverTimeMillis; }
        public void setFailoverTimeMillis(long failoverTimeMillis) { this.failoverTimeMillis = failoverTimeMillis; }
        public String getAppSelector() { return appSelector; }
        public void setAppSelector(String appSelector) { this.appSelector = appSelector; }
    }

    public static class Alarm {
        private long iteratorAgeThresholdMs = 60000;
        private int evaluationPeriods = 2;
        private int period = 60;

        public long getIteratorAgeThresholdMs() { return iteratorAgeThresholdMs; }
        public void setIteratorAgeThresholdMs(long iteratorAgeThresholdMs) { this.iteratorAgeThresholdMs = iteratorAgeThresholdMs; }
        public int getEvaluationPeriods() { return evaluationPeriods; }
        public void setEvaluationPeriods(int evaluationPeriods) { this.evaluationPeriods = evaluationPeriods; }
        public int getPeriod() { return period; }
        public void setPeriod(int period) { this.period = period; }
    }
}
