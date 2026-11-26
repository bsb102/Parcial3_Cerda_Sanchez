package cl.duoc.backendskatesapp.streaming.analytics;

import java.math.BigDecimal;
import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "kinesis_analytics_metrics")
public class AnalyticsMetric {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    @Column(nullable = false, length = 36)
    private String eventId;
    @Column(nullable = false, length = 40)
    private String eventType;
    @Column(length = 26)
    private String buyOrder;
    private BigDecimal amount;
    private Long skateId;
    private Integer stockAfter;
    @Column(nullable = false)
    private Instant occurredAt;

    protected AnalyticsMetric() { }

    public AnalyticsMetric(String eventId, String eventType, String buyOrder, BigDecimal amount,
            Long skateId, Integer stockAfter, Instant occurredAt) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.buyOrder = buyOrder;
        this.amount = amount;
        this.skateId = skateId;
        this.stockAfter = stockAfter;
        this.occurredAt = occurredAt;
    }
}
