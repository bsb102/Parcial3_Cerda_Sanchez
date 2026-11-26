package cl.duoc.backendskatesapp.streaming.analytics;

import org.springframework.data.jpa.repository.JpaRepository;

public interface AnalyticsMetricRepository extends JpaRepository<AnalyticsMetric, Long> { }
