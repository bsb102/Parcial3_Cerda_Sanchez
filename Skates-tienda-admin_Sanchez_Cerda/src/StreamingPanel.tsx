import { useCallback, useEffect, useState } from "react";
import {
  obtenerLeasesKinesis,
  obtenerRetrasoKinesis,
  obtenerStreamsKinesis,
  type KinesisConsumerLease,
  type KinesisIteratorAge,
  type KinesisStreamStatus,
} from "./api";

export default function StreamingPanel() {
  const [streams, setStreams] = useState<KinesisStreamStatus[]>([]);
  const [leases, setLeases] = useState<KinesisConsumerLease[]>([]);
  const [delays, setDelays] = useState<KinesisIteratorAge[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      const [nextStreams, nextLeases] = await Promise.all([
        obtenerStreamsKinesis(),
        obtenerLeasesKinesis(),
      ]);
      const nextDelays = await Promise.all(
        nextStreams.map((stream) => obtenerRetrasoKinesis(stream.name)),
      );
      setStreams(nextStreams);
      setLeases(nextLeases);
      setDelays(nextDelays);
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
        const nextDelays = await Promise.all(
          nextStreams.map((stream) => obtenerRetrasoKinesis(stream.name)),
        );
        if (active) {
          setStreams(nextStreams);
          setLeases(nextLeases);
          setDelays(nextDelays);
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

  if (loading) return <p className="messaging-message">Consultando estado de Kinesis...</p>;

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
              <p className="messaging-message">No hay leases activos. Las tablas aparecen cuando arranca cada aplicación KCL.</p>
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
          <p className="messaging-note">
            Kinesis está deshabilitado en el backend hasta configurar KINESIS_ENABLED y los recursos AWS.
            La vista se actualiza cada 15 segundos.
          </p>
        </>
      )}
    </div>
  );
}
