import { MeterProvider, PeriodicExportingMetricReader } from '@opentelemetry/sdk-metrics';
import { resourceFromAttributes } from '@opentelemetry/resources';
import { OTLPMetricExporter } from '@opentelemetry/exporter-metrics-otlp-http';
import { connect } from '../src';

async function main(): Promise<void> {
  // Uses OTEL_EXPORTER_OTLP_ENDPOINT (default http://localhost:4318).
  const provider = new MeterProvider({
    resource: resourceFromAttributes({ 'service.name': 'postgremq-ts-example' }),
    readers: [
      new PeriodicExportingMetricReader({
        exporter: new OTLPMetricExporter(),
        exportIntervalMillis: 5000,
      }),
    ],
  });
  let connection: Awaited<ReturnType<typeof connect>> | undefined;
  let timer: NodeJS.Timeout | undefined;
  try {
    connection = await connect({
      connectionString:
        process.env.DATABASE_URL ??
        'postgres://postgres:postgremq@localhost:55432/postgremq?sslmode=disable',
      meterProvider: provider,
      shutdownTimeoutMs: 5000,
    });
    await connection.createTopic('metrics_ts');
    await connection.createQueue('metrics_ts', 'metrics_ts', false);
    let finish!: () => void;
    let fail!: (err: unknown) => void;
    const done = new Promise<void>((resolve, reject) => {
      finish = resolve;
      fail = reject;
    });
    // Attach the rejection handler before publishing so timeout cannot become
    // an unhandled rejection while a database call is pending.
    const completed = done.then(
      () => undefined,
      (err) => err as Error,
    );
    timer = setTimeout(() => fail(new Error('Consumer timed out')), 30000);
    const consumer = connection.consumeHandler(
      'metrics_ts',
      async (msg) => {
        try {
          await msg.ack();
          finish();
        } catch (err) {
          fail(err);
          throw err;
        }
      },
      { maxInFlight: 1 },
    );
    await connection.publish('metrics_ts', { example: 'metrics' });
    const error = await completed;
    if (error) throw error;
    await consumer.stop();
    await connection.close();
    await provider.forceFlush();
    console.log('TypeScript client metrics exported');
  } finally {
    if (timer) clearTimeout(timer);
    await connection?.close();
    await provider.shutdown();
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
