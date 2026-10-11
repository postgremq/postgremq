import { metrics as otelMetrics } from '@opentelemetry/api';
import { test, expect } from '@jest/globals';
import {
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader,
  AggregationTemporality,
} from '@opentelemetry/sdk-metrics';
import { readFileSync } from 'fs';
import { join } from 'path';
import { Connection } from '../connection';
import { Message } from '../message';
import { createIsolatedTestConnection, getSharedTestDatabase } from './helpers';
import { LeaseLostError, QueueNotFoundError } from '../errors';

const contract = JSON.parse(
  readFileSync(join(__dirname, '../../../observability/client-contract.json'), 'utf8'),
);

function telemetry() {
  const exporter = new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE);
  const reader = new PeriodicExportingMetricReader({ exporter, exportIntervalMillis: 60000 });
  return { exporter, provider: new MeterProvider({ readers: [reader] }) };
}

test('metrics contract with transactions, empty receive, redelivery and failed settlement', async () => {
  const iso = await createIsolatedTestConnection(await getSharedTestDatabase());
  const { exporter, provider } = telemetry();
  const c = new Connection({ pool: iso.pool, meterProvider: provider });
  try {
    await c.connect();
    await c.createTopic('metrics');
    await c.createQueue('q', 'metrics', false);
    await c.publish('metrics', {});
    const tx = await iso.pool.connect();
    try {
      await tx.query('BEGIN');
      await c.publishWithTransaction(tx, 'metrics', {});
      await tx.query('ROLLBACK');
      await expect(c.publish('missing', {})).rejects.toBeInstanceOf(QueueNotFoundError);
      const take = async () => {
        const rows = await c.consumeMessages('q', 30, 1);
        expect(rows).toHaveLength(1);
        const r = rows[0];
        return new Message(
          r.message_id,
          'q',
          r.payload,
          r.consumer_token,
          r.delivery_attempts,
          r.vt,
          r.published_at,
          () => {},
          {
            ack: c.ackMessage.bind(c),
            nack: c.nackMessage.bind(c),
            release: c.releaseMessage.bind(c),
            setVt: c.setMessageVt.bind(c),
          },
        );
      };
      let msg = await take();
      await msg.setVt(30);
      await tx.query('BEGIN');
      await msg.ackWithTransaction(tx);
      await tx.query('ROLLBACK');
      await expect(msg.ack()).rejects.toThrow('already been processed');
      await iso.pool.query(
        "UPDATE postgremq.queue_messages SET vt=clock_timestamp()-interval '1 second'",
      );
      msg = await take();
      await msg.nack();
      msg = await take();
      await msg.release();
      await iso.pool.query('DELETE FROM postgremq.queue_messages');
      expect(await c.consumeMessages('q', 30, 1)).toEqual([]);
      await iso.pool.query("SELECT postgremq.publish_message('metrics','{}')");
      msg = await take();
      await iso.pool.query(
        'UPDATE postgremq.queue_messages SET consumer_token=gen_random_uuid()::text',
      );
      await expect(msg.ack()).rejects.toBeInstanceOf(LeaseLostError);
    } finally {
      await tx.query('ROLLBACK');
      tx.release();
    }
    await provider.forceFlush();
    const scopes = exporter.getMetrics().flatMap((r) => r.scopeMetrics);
    expect(scopes).toHaveLength(1);
    expect(scopes[0].scope.name).toBe(contract.scope);
    expect(scopes[0].scope.version).toBe(contract.version);
    for (const metric of scopes[0].metrics)
      expect(metric.descriptor.unit).toBe(contract.metrics[metric.descriptor.name]);
    const operations = scopes[0].metrics.find(
      (m) => m.descriptor.name === 'messaging.client.operation.duration',
    )!;
    expect(operations.dataPoints).toHaveLength(contract.scenario_operations.length);
    for (const expected of contract.scenario_operations) {
      const attrs: Record<string, string | boolean> = {
        'messaging.system': 'postgremq',
        'messaging.operation.name': expected.name,
        'messaging.operation.type': expected.type,
        'messaging.destination.name': expected.destination,
        'postgremq.transaction': expected.transaction,
        ...(expected.error ? { 'error.type': expected.error } : {}),
      };
      const point = operations.dataPoints.find((p) =>
        Object.keys(attrs).every((k) => p.attributes[k] === attrs[k]),
      );
      expect(point).toBeDefined();
      expect(point!.attributes).toEqual(attrs);
      const histogram = point!.value as {
        count: number;
        sum: number;
        buckets: { boundaries: number[] };
      };
      expect(histogram.count).toBe(expected.count);
      expect(histogram.sum).toBeGreaterThanOrEqual(0);
      expect(histogram.buckets.boundaries).toEqual(contract.histogram_boundaries);
    }
    const sent = scopes[0].metrics.find(
      (m) => m.descriptor.name === 'messaging.client.sent.messages',
    )!;
    expect(sent.dataPoints).toHaveLength(contract.sent.length);
    for (const expected of contract.sent) {
      const attrs = {
        'messaging.system': 'postgremq',
        'messaging.operation.name': 'publish',
        'messaging.operation.type': 'send',
        'messaging.destination.name': expected.destination,
        'postgremq.transaction': expected.transaction,
        ...(expected.error ? { 'error.type': expected.error } : {}),
      };
      const point = sent.dataPoints.find(
        (p) =>
          p.attributes['messaging.destination.name'] === expected.destination &&
          p.attributes['postgremq.transaction'] === expected.transaction,
      );
      expect(point?.attributes).toEqual(attrs);
      expect(point?.value).toBe(expected.count);
    }
    const received = scopes[0].metrics.find(
      (m) => m.descriptor.name === 'messaging.client.consumed.messages',
    )!;
    expect(
      received.dataPoints.find((p) => p.attributes['postgremq.redelivered'] === false)!.value,
    ).toBe(contract.received.first);
    expect(
      received.dataPoints.find((p) => p.attributes['postgremq.redelivered'] === true)!.value,
    ).toBe(contract.received.redelivered);
  } finally {
    await c.close();
    await provider.shutdown();
    await iso.connection.close();
    await iso.dropDatabase();
  }
});

test.each(['', 'handler_error', 'cancelled'])(
  'handler metrics balance active callbacks: %s',
  async (outcome) => {
    const { HandlerConsumer } = await import('../handler-consumer');
    const { exporter, provider } = telemetry();
    const c = new Connection({ meterProvider: provider });
    let unblock!: () => void;
    let entered!: () => void;
    const gate = new Promise<void>((resolve) => {
      unblock = resolve;
    });
    const started = new Promise<void>((resolve) => {
      entered = resolve;
    });
    const msg = new Message(1, 'q', {}, 'token', 1, new Date(), new Date(), () => {}, {
      ack: async () => {},
      nack: async () => {},
      release: async () => {},
      setVt: async () => new Date(),
    });
    // Explicit settlement must not end processing metrics while the callback runs.
    await msg.ack();
    const hc = new HandlerConsumer(
      c,
      {} as import('../consumer').Consumer,
      async () => {
        entered();
        await gate;
        if (outcome === 'handler_error') throw new Error('private handler detail');
      },
      1,
    );
    const running = hc['runHandler'](msg);
    try {
      await started;
      await provider.forceFlush();
      const during = exporter
        .getMetrics()
        .flatMap((r) => r.scopeMetrics)
        .flatMap((s) => s.metrics);
      expect(
        during.find((m) => m.descriptor.name === 'postgremq.client.handlers.active')!.dataPoints[0]
          .value,
      ).toBe(1);
      expect(
        during.find((m) => m.descriptor.name === 'messaging.process.duration'),
      ).toBeUndefined();
      if (outcome === 'cancelled') msg._cancel();
      unblock();
      await running;
      exporter.reset();
      await provider.forceFlush();
      const after = exporter
        .getMetrics()
        .flatMap((r) => r.scopeMetrics)
        .flatMap((s) => s.metrics);
      expect(
        after.find((m) => m.descriptor.name === 'postgremq.client.handlers.active')!.dataPoints[0]
          .value,
      ).toBe(0);
      const process = after.find((m) => m.descriptor.name === 'messaging.process.duration')!;
      expect(process.descriptor.unit).toBe(contract.metrics[process.descriptor.name]);
      expect(process.dataPoints).toHaveLength(1);
      expect((process.dataPoints[0].value as { count: number }).count).toBe(1);
      expect(process.dataPoints[0].attributes['error.type'] ?? '').toBe(outcome);
    } finally {
      unblock();
      await running;
      await c.close();
      await provider.shutdown();
    }
  },
);

test('renewal metrics ignore deregistered deliveries and count retired entries once', async () => {
  const { exporter, provider } = telemetry();
  const c = new Connection({ meterProvider: provider });
  try {
    // Drive the existing renewal actor with a successful batch missing its row.
    // No database/poll timer is needed to test apply behavior.
    const e = {
      queue: 'q',
      id: 1,
      token: 't',
      vtSec: 30,
      threshold: 0.5,
      nextExtensionTime: new Date(0),
      expiresAt: new Date(Date.now() + 30000),
      cancel: () => {},
      onExtended: () => {},
    };
    c['extenderEntries'].set('q\0' + '1\0t', e);
    c.setVtBatchMulti = async () => [];
    await c['flushExtender']();
    await c['flushExtender']();
    await provider.forceFlush();
    const metric = exporter
      .getMetrics()
      .flatMap((r) => r.scopeMetrics)
      .flatMap((s) => s.metrics)
      .find((m) => m.descriptor.name === 'postgremq.client.renewal.lost')!;
    expect(metric.descriptor.unit).toBe(contract.metrics[metric.descriptor.name]);
    expect(metric.dataPoints[0].value).toBe(1);
    expect(metric.dataPoints[0].attributes).toEqual({
      'messaging.system': 'postgremq',
      'messaging.destination.name': 'q',
    });
  } finally {
    await c.close();
    await provider.shutdown();
  }
});

test('operation duration counts a retried publication once, including backoff', async () => {
  const iso = await createIsolatedTestConnection(await getSharedTestDatabase());
  const { exporter, provider } = telemetry();
  const c = new Connection({ pool: iso.pool, meterProvider: provider });
  try {
    await c.connect();
    await c.createTopic('metrics');
    await iso.pool.query(
      readFileSync(join(__dirname, '../../../observability/retry-once.sql'), 'utf8'),
    );
    await c.publish('metrics', {});
    expect(
      (await iso.pool.query('SELECT last_value FROM public.metrics_retry_probe')).rows[0]
        .last_value,
    ).toBe('2');
    await provider.forceFlush();
    const metrics = exporter
      .getMetrics()
      .flatMap((r) => r.scopeMetrics)
      .flatMap((s) => s.metrics);
    expect(metrics).toHaveLength(2);
    const duration = metrics.find(
      (m) => m.descriptor.name === 'messaging.client.operation.duration',
    )!;
    const sent = metrics.find((m) => m.descriptor.name === 'messaging.client.sent.messages')!;
    expect(sent.dataPoints).toHaveLength(2);
    expect(
      Object.fromEntries(sent.dataPoints.map((p) => [p.attributes['error.type'] ?? '', p.value])),
    ).toEqual({ '': 1, other: 1 });
    expect(duration.dataPoints).toHaveLength(1);
    const point = duration.dataPoints[0];
    expect(point.attributes['error.type']).toBeUndefined();
    expect((point.value as { count: number }).count).toBe(1);
    expect((point.value as { sum: number }).sum).toBeGreaterThanOrEqual(0.1);
  } finally {
    await c.close();
    await provider.shutdown();
    await iso.connection.close();
    await iso.dropDatabase();
  }
});

test.each([false, true])(
  'metrics are optional and provider remains application-owned: %s',
  async (enabled) => {
    const iso = await createIsolatedTestConnection(await getSharedTestDatabase());
    const { exporter, provider } = telemetry();
    expect(otelMetrics.setGlobalMeterProvider(provider)).toBe(true);
    const c = new Connection({ pool: iso.pool, ...(enabled ? { meterProvider: provider } : {}) });
    try {
      await c.connect();
      await c.createTopic('optional');
      await c.createQueue('q', 'optional', false);
      await c.publish('optional', {});
      const messages = await c.consumeMessages('q', 30, 1);
      expect(messages).toHaveLength(1);
      await c.ackMessage('q', messages[0].message_id, messages[0].consumer_token);
      // Locally rejected/unsent messages must not contribute to sent.messages.
      const circular: any = {};
      circular.self = circular;
      await expect(c.publish('optional', circular)).rejects.toThrow();
      await c.close();
      await expect(c.publish('optional', {})).rejects.toThrow();
      provider.getMeter('application').createCounter('application.probe').add(1);
      await provider.forceFlush();
      const scopes = exporter.getMetrics().flatMap((r) => r.scopeMetrics);
      expect(
        scopes.find((s) => s.scope.name === 'application')?.metrics[0].dataPoints[0].value,
      ).toBe(1);
      const client = scopes.find((s) => s.scope.name === 'postgremq');
      expect(!!client).toBe(enabled);
      if (enabled) {
        const sent = client!.metrics.find(
          (m) => m.descriptor.name === 'messaging.client.sent.messages',
        )!;
        expect(sent.dataPoints).toHaveLength(1);
        expect(sent.dataPoints[0].value).toBe(1);
      }
    } finally {
      otelMetrics.disable();
      await c.close();
      await provider.shutdown();
      await iso.connection.close();
      await iso.dropDatabase();
    }
  },
);
