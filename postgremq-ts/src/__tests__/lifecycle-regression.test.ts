import { test, expect, jest } from '@jest/globals';
import { Connection } from '../connection';
import { Consumer } from '../consumer';
import { createDeferred, sleep, messageId } from '../utils';

function client(timeout = 50): Connection {
  const c = new Connection({
    pool: { on() {}, connect: jest.fn() } as any,
    shutdownTimeoutMs: timeout,
  });
  (c as any).connected = true;
  jest.spyOn(c, 'resolveQueueGeneration').mockResolvedValue('test-generation');
  return c;
}
const delivery = (token: string) => ({
  message_id: 1,
  payload: {},
  consumer_token: token,
  delivery_attempts: 1,
  vt: new Date(Date.now() + 30000),
  published_at: new Date(),
});

test('old settlement preserves the new delivery heartbeat', async () => {
  const c = client();
  const register = (token: string) =>
    c.extenderRegister({
      queue: 'q',
      id: 1,
      token,
      vtSec: 30,
      threshold: 0.5,
      vt: new Date(Date.now() + 30000),
      cancel: jest.fn(),
      onExtended: jest.fn(),
    });
  register('old');
  register('new');
  c.extenderDeregister('q', 1, 'old');
  expect(Array.from((c as any).extenderEntries.values()).map((e: any) => e.token)).toEqual(['new']);
  await c.close();
});

test('a fetch arriving after stop cannot register an orphan heartbeat', async () => {
  const c = client(20);
  const fetch = createDeferred<any[]>();
  const claimed = createDeferred<void>();
  jest.spyOn(c, 'consumeMessages').mockImplementation(() => {
    claimed[1]();
    return fetch[0];
  });
  const release = jest.spyOn(c, 'releaseMessage').mockResolvedValue();
  const consumer = new Consumer('q', c, { topic: 't', batchSize: 1 });
  const next = consumer.messages().next();
  await claimed[0];
  await consumer.stop();
  fetch[1]([delivery('late')]);
  await sleep(10);
  expect(release).toHaveBeenCalledWith('q', 1, 'late');
  expect((c as any).extenderEntries.size).toBe(0);
  expect((consumer as any).messageBuffer).toHaveLength(0);
  expect((await next).done).toBe(true);
  await c.close();
});

test('forced stop abandons an active delivery without releasing it', async () => {
  const c = client(30);
  jest
    .spyOn(c, 'consumeMessages')
    .mockResolvedValueOnce([delivery('active')])
    .mockResolvedValue([]);
  jest.spyOn(c, 'getNextVisibleTime').mockResolvedValue(new Date(Date.now() + 60000));
  const release = jest.spyOn(c, 'releaseMessage').mockResolvedValue();
  const consumer = new Consumer('q', c, { topic: 't', batchSize: 1 });
  const { value: message } = await consumer.messages().next();
  const stop = consumer.stop();
  expect(consumer.stop()).toBe(stop);
  await stop;
  expect(message.signal.aborted).toBe(true);
  expect(release).not.toHaveBeenCalled();
  expect((c as any).extenderEntries.size).toBe(0);
  await c.close();
});

test('stop unregisters a consumer that was never started', async () => {
  const c = client();
  const consumer = new Consumer('q', c, { topic: 't' });
  c.registerConsumer(consumer);
  await consumer.stop();
  expect((c as any).consumers.size).toBe(0);
  await c.close();
});

test('close callers share completion and no new consumers enter drain', async () => {
  const c = client();
  const drain = createDeferred<void>();
  c.registerConsumer({ stop: () => drain[0] });
  const close = c.close();
  expect(c.close()).toBe(close);
  expect(() => c.consume('q', { topic: 't' })).toThrow();
  drain[1]();
  await close;
});

test('BIGINT IDs cannot silently round', () => {
  expect(messageId('9007199254740991')).toBe(Number.MAX_SAFE_INTEGER);
  expect(() => messageId('9007199254740993')).toThrow(RangeError);
});

test('notification loop survives repeated acquisition failures', async () => {
  const { EventEmitter } = await import('events');
  const socket = Object.assign(new EventEmitter(), {
    query: jest.fn(async () => ({ rows: [] })),
    release: jest.fn(),
  });
  let attempts = 0;
  const pool = {
    on() {},
    async connect() {
      if (++attempts < 3) throw new Error('database unavailable');
      return socket;
    },
  } as any;
  const c = new Connection({ pool, shutdownTimeoutMs: 100 });
  (c as any).connected = true;
  const original = (Connection as any).NOTIFY_RECONNECT_BASE_MS;
  (Connection as any).NOTIFY_RECONNECT_BASE_MS = 5;
  try {
    const unsubscribe = c.subscribeForConsumer('q', 't', () => {});
    for (let i = 0; i < 100 && socket.query.mock.calls.length < 2; i++) await sleep(5);
    expect(attempts).toBeGreaterThanOrEqual(3);
    expect(socket.query.mock.calls.length).toBe(2);
    unsubscribe();
    await c.close();
    expect(socket.release).toHaveBeenCalledTimes(1);
  } finally {
    (Connection as any).NOTIFY_RECONNECT_BASE_MS = original;
    await c.close();
  }
});

test('failed queue deletion does not stop its keepalive', async () => {
  const c = client();
  const entry = {
    generation: 'g',
    intervalSec: 60,
    nextAt: new Date(Date.now() + 30000),
    expiresAt: new Date(Date.now() + 60000),
  };
  (c as any).keepAliveEntries.set('q', entry);
  jest.spyOn(c, 'executeWithRetry').mockRejectedValue(new Error('queue has DLQ entries'));
  await expect(c.deleteQueue('q')).rejects.toThrow('DLQ');
  expect((c as any).keepAliveEntries.get('q')).toBe(entry);
  await c.close();
});

test('an ambiguous publish response is not automatically retried', async () => {
  const query = jest.fn(async () => {
    throw Object.assign(new Error('response lost'), { code: 'ECONNRESET' });
  });
  const c = new Connection({
    pool: {
      on() {},
      async connect() {
        return { query, release() {} };
      },
    } as any,
  });
  (c as any).connected = true;
  await expect(c.publish('t', {})).rejects.toThrow('response lost');
  expect(query).toHaveBeenCalledTimes(1);
  await c.close();
});

test('handler topic errors are synchronous and do not leave a registered consumer', async () => {
  const c = client();
  expect(() => c.consumeHandler('unknown', async () => {})).toThrow('topic unknown');
  expect((c as any).consumers.size).toBe(0);
  await c.close();
});

test('a stopped consumer cannot start a claim after late queue binding', async () => {
  const c = client(20);
  const binding = createDeferred<string>();
  jest.spyOn(c, 'resolveQueueGeneration').mockImplementation(() => binding[0]);
  const claim = jest.spyOn(c, 'consumeMessages').mockResolvedValue([]);
  const consumer = new Consumer('q', c, { topic: 't' });
  const next = consumer.messages().next();
  await consumer.stop();
  binding[1]('generation');
  await sleep(5);
  expect(claim).not.toHaveBeenCalled();
  expect((await next).done).toBe(true);
  await c.close();
});

test('unsafe IDs are rejected before destructive operations', async () => {
  const c = client();
  await expect(c.deleteQueueMessage('q', Number.MAX_SAFE_INTEGER + 1)).rejects.toThrow(RangeError);
  await c.close();
});
