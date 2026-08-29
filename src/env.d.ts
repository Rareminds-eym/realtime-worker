/**
 * Environment bindings for Realtime Worker
 */
export interface Env {
  // Durable Object binding
  REALTIME_HUB: DurableObjectNamespace;

  // Queue binding
  REALTIME_EVENTS_QUEUE: Queue<unknown>;

  // Service bindings
  SSO_SERVICE: any;
}
