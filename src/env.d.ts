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

  // Skillpassport API URL for HTTP calls
  SKILLPASSPORT_URL?: string;

  // Internal webhook secret for service-to-service authentication
  INTERNAL_WEBHOOK_SECRET?: string;
}
