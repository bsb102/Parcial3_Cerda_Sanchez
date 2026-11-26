package cl.duoc.backendskatesapp.streaming.notifications;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "kinesis_derived_notifications")
public class DerivedNotification {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    @Column(nullable = false, length = 36)
    private String eventId;
    @Column(nullable = false, length = 40)
    private String eventType;
    @Column(nullable = false, length = 500)
    private String message;
    @Column(nullable = false)
    private Instant createdAt;

    protected DerivedNotification() { }

    public DerivedNotification(String eventId, String eventType, String message, Instant createdAt) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.message = message;
        this.createdAt = createdAt;
    }
}
