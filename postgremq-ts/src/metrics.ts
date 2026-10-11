import { Attributes, Histogram, Counter, UpDownCounter, MeterProvider } from '@opentelemetry/api';
import { performance } from 'perf_hooks';
import {
  ConnectionClosedError,
  LeaseLostError,
  QueueNotFoundError,
  QueueFatalError,
  ValidationError,
  mapDbError,
} from './errors';

function attributes(operation: string, destination: string): Attributes {
  const type =
    operation === 'publish'
      ? 'send'
      : operation === 'consume'
        ? 'receive'
        : ['ack', 'nack', 'release'].includes(operation)
          ? 'settle'
          : operation;
  return {
    'messaging.system': 'postgremq',
    'messaging.operation.name': operation,
    'messaging.operation.type': type,
    ...(destination ? { 'messaging.destination.name': destination } : {}),
  };
}

function errorType(error: unknown): string {
  if (error instanceof LeaseLostError) return 'lease_lost';
  if (error instanceof QueueNotFoundError || error instanceof QueueFatalError)
    return 'queue_not_found';
  if (error instanceof ValidationError) return 'validation';
  if (error instanceof ConnectionClosedError) return 'connection_closed';
  if (error instanceof Error && error.name === 'AbortError') return 'cancelled';
  return 'other';
}

/** Internal implementation of the shared metrics contract in docs/observability.md. */
export class ClientMetrics {
  private readonly renewalLost?: Counter;
  private readonly duration?: Histogram;
  private readonly process?: Histogram;
  private readonly consumed?: Counter;
  private readonly sent?: Counter;
  private readonly active?: UpDownCounter;

  constructor(provider?: MeterProvider) {
    const meter = provider?.getMeter('postgremq', '1');
    const options = {
      unit: 's',
      advice: {
        explicitBucketBoundaries: [
          0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10,
        ],
      },
    };
    this.duration = meter?.createHistogram('messaging.client.operation.duration', options);
    this.process = meter?.createHistogram('messaging.process.duration', options);
    this.sent = meter?.createCounter('messaging.client.sent.messages', { unit: '{message}' });
    this.consumed = meter?.createCounter('messaging.client.consumed.messages', {
      unit: '{message}',
    });
    this.renewalLost = meter?.createCounter('postgremq.client.renewal.lost', { unit: '{message}' });
    this.active = meter?.createUpDownCounter('postgremq.client.handlers.active', {
      unit: '{handler}',
    });
  }

  async operation<T>(
    operation: string,
    destination: string,
    transaction: boolean,
    fn: () => Promise<T>,
  ): Promise<T> {
    if (!this.duration) return fn();
    const start = performance.now();
    const attrs = { ...attributes(operation, destination), 'postgremq.transaction': transaction };
    try {
      return await fn();
    } catch (error) {
      Object.assign(attrs, { 'error.type': errorType(error) });
      throw error;
    } finally {
      this.duration.record((performance.now() - start) / 1000, attrs);
    }
  }

  // Observe the actual driver query attempt, after payload serialization.
  async send<T>(topic: string, transaction: boolean, fn: () => Promise<T>): Promise<T> {
    if (!this.sent) return fn();
    const attrs: Attributes = {
      ...attributes('publish', topic),
      'postgremq.transaction': transaction,
    };
    try {
      return await fn();
    } catch (error) {
      attrs['error.type'] = errorType(mapDbError(error));
      throw error;
    } finally {
      this.sent.add(1, attrs);
    }
  }

  recordConsumed(queue: string, messages: Array<{ delivery_attempts: number }>): void {
    if (!this.consumed) return;
    const redelivered = messages.filter((m) => m.delivery_attempts > 1).length;
    const attrs = attributes('consume', queue);
    if (messages.length > redelivered)
      this.consumed.add(messages.length - redelivered, {
        ...attrs,
        'postgremq.redelivered': false,
      });
    if (redelivered) this.consumed.add(redelivered, { ...attrs, 'postgremq.redelivered': true });
  }

  recordRenewalLost(queue: string): void {
    this.renewalLost?.add(1, {
      'messaging.system': 'postgremq',
      'messaging.destination.name': queue,
    });
  }

  startHandler(queue: string): (errorType: string) => void {
    if (!this.process) return () => {};
    const start = performance.now();
    const attrs = attributes('process', queue);
    this.active?.add(1, attrs);
    return (code) => {
      this.active?.add(-1, attrs);
      this.process?.record((performance.now() - start) / 1000, {
        ...attrs,
        ...(code ? { 'error.type': code } : {}),
      });
    };
  }
}
