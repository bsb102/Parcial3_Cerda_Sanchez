import { useCallback, useEffect, useState } from "react";
import {
  obtenerLeasesKinesis,
  obtenerEventosKinesisProcesados,
  publicarEventoPruebaKinesis,
  obtenerRetrasoKinesis,
  obtenerStreamsKinesis,
  type KinesisConsumerLease,
  type KinesisIteratorAge,
  type KinesisProcessedEvent,
  type KinesisPublishedEvent,
  type KinesisTestEventType,
  type KinesisStreamStatus,
} from "./api";

export default function StreamingPanel() {
  const [streams, setStreams] = useState<KinesisStreamStatus[]>([]);
  const [leases, setLeases] = useState<KinesisConsumerLease[]>([]);
  const [processedEvents, setProcessedEvents] = useState<KinesisProcessedEvent[]>([]);
  const [delays, setDelays] = useState<KinesisIteratorAge[]>([]);
  const [lastPublished, setLastPublished] = useState<KinesisPublishedEvent | null>(null);
  const [publishing, setPublishing] = useState<KinesisTestEventType | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      const [nextStreams, nextLeases] = await Promise.all([
        obtenerStreamsKinesis(),
        obtenerLeasesKinesis(),
      ]);
      const [nextDelays, nextProcessedEvents] = await Promise.all([
        Promise.all(nextStreams.map((stream) => obtenerRetrasoKinesis(stream.name))),
        obtenerEventosKinesisProcesados(),
      ]);
      setStreams(nextStreams);
      setLeases(nextLeases);
      setDelays(nextDelays);
      setProcessedEvents(nextProcessedEvents);
      setError(null);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "No se pudo consultar Kinesis.");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    let active = true;
    const load = async () => {
      try {
        const [nextStreams, nextLeases] = await Promise.all([
          obtenerStreamsKinesis(),
          obtenerLeasesKinesis(),
        ]);
        const [nextDelays, nextProcessedEvents] = await Promise.all([
          Promise.all(nextStreams.map((stream) => obtenerRetrasoKinesis(stream.name))),
          obtenerEventosKinesisProcesados(),
        ]);
        if (active) {
          setStreams(nextStreams);
          setLeases(nextLeases);
          setDelays(nextDelays);
          setProcessedEvents(nextProcessedEvents);
          setError(null);
        }
      } catch (cause) {
        if (active) setError(cause instanceof Error ? cause.message : "No se pudo consultar Kinesis.");
      } finally {
        if (active) setLoading(false);
      }
    };
    void load();
    const interval = window.setInterval(() => void load(), 15000);
    return () => {
      active = false;
      window.clearInterval(interval);
    };
  }, []);

  const publishTestEvent = async (eventType: KinesisTestEventType) => {
    setPublishing(eventType);
    setError(null);
    try {
      const publishedEvent = await publicarEventoPruebaKinesis(eventType);
      setLastPublished(publishedEvent);
      await refresh();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "No se pudo publicar el evento de prueba.");
    } finally {
      setPublishing(null);
    }
  };

  if (loading) return <p className="messaging-message">Consultando estado de Kinesis...</p>;
  const publishedEventConsumers = lastPublished
    ? processedEvents
        .filter((event) => event.eventId === lastPublished.eventId)
        .map((event) => event.consumerName)
    : [];

  return (
    <div className="messaging-panel">
      <div className="messaging-heading">
        <div>
          <h3>Streaming (Kinesis)</h3>
          <p>Estado de streams, leases KCL y retraso del consumidor.</p>
        </div>
        <button type="button" onClick={() => void refresh()}>Actualizar</button>
      </div>
      {error && <p className="messaging-error" role="alert">{error}</p>}
      {!error && (
        <>
          <section className="messaging-events">
            <h3>Publicar evento de prueba</h3>
            <p className="messaging-message">
              Envía un evento sintético al mismo stream de órdenes o inventario; no modifica pedidos ni productos.
            </p>
            <div className="kinesis-test-actions">
              <button type="button" disabled={publishing !== null} onClick={() => void publishTestEvent("ORDER")}>
                {publishing === "ORDER" ? "Publicando..." : "Probar evento de orden"}
              </button>
              <button type="button" disabled={publishing !== null} onClick={() => void publishTestEvent("INVENTORY")}>
                {publishing === "INVENTORY" ? "Publicando..." : "Probar evento de inventario"}
              </button>
            </div>
            {lastPublished && (
              <p className="messaging-note" role="status">
                {publishedEventConsumers.length > 0
                  ? `Evento ${lastPublished.eventId} procesado por: ${publishedEventConsumers.join(", ")}.`
                  : `Evento ${lastPublished.eventId} publicado; aún no aparece como procesado.`}
              </p>
            )}
          </section>
          <div className="messaging-queues">
            {streams.map((stream) => (
              <article className="messaging-queue-card" key={stream.name}>
                <header>
                  <div><small>Stream</small><h4>{stream.name}</h4></div>
                  <span className="messaging-status is-running">{stream.status}</span>
                </header>
                <dl>
                  <div><dt>Shards</dt><dd>{stream.shardCount ?? "—"}</dd></div>
                  <div><dt>Retención</dt><dd>{stream.retentionHours ?? "—"} h</dd></div>
                  <div><dt>Capacidad</dt><dd>{stream.capacityMode}</dd></div>
                  <div><dt>Retraso máximo</dt><dd>{delays.find((item) => item.streamName === stream.name)?.iteratorAgeMilliseconds ?? "—"} ms</dd></div>
                </dl>
              </article>
            ))}
          </div>
          <section className="messaging-events">
            <h3>Consumidores KCL</h3>
            {leases.length === 0 ? (
              <p className="messaging-message">No hay leases visibles para las aplicaciones KCL configuradas.</p>
            ) : (
              <div className="messaging-event-list">
                {leases.map((lease) => (
                  <article className="messaging-event" key={`${lease.applicationName}:${lease.shardId}`}>
                    <div><strong>{lease.applicationName}</strong><span>Shard {lease.shardId}</span></div>
                    <span>Owner: {lease.leaseOwner || "sin asignar"}</span>
                    <code>Checkpoint: {lease.checkpoint || "sin checkpoint"}</code>
                  </article>
                ))}
              </div>
            )}
          </section>
          <section className="messaging-events">
            <h3>Eventos procesados recientemente</h3>
            {processedEvents.length === 0 ? (
              <p className="messaging-message">No hay eventos KCL persistidos recientemente. Los leases no confirman procesamiento.</p>
            ) : (
              <div className="messaging-event-list">
                {processedEvents.map((event) => (
                  <article className="messaging-event" key={`${event.consumerName}:${event.eventId}`}>
                    <div><strong>{event.eventType}</strong><span>Consumidor: {event.consumerName}</span></div>
                    <code>{event.eventId}</code>
                    <time dateTime={event.processedAt}>{new Date(event.processedAt).toLocaleString()}</time>
                  </article>
                ))}
              </div>
            )}
          </section>
          <p className="messaging-note">
            El historial confirma procesamiento persistido por KCL; las leases solo indican asignación de shards.
            La vista se actualiza cada 15 segundos.
          </p>
        </>
      )}
    </div>
  );
}
