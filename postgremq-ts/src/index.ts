/**
 * PostgreMQ TypeScript Client
 * Main entry point for the PostgreMQ client
 */

// Export main interfaces and types
export { IConnection as Connection } from './types';
export { Message } from './message';
export { Consumer } from './consumer';
export { HandlerConsumer, MessageHandler } from './handler-consumer';

// Export public types
export {
  ConnectionOptions,
  PublishOptions,
  QueueOptions,
  ConsumerOptions,
  HandlerConsumerOptions,
  MessageOptions,
  QueueInfo,
  QueueStatistics,
  DLQMessage,
  QueueMessage,
  PublishedMessage,
  Transaction
} from './types';

// Export typed errors so application code can `instanceof LeaseLostError`
// or check `err.code === 'PMQ01'` without importing internal modules.
export {
  LeaseLostError,
  QueueNotFoundError,
  ValidationError,
  ConnectionClosedError,
  QueueFatalError,
  DirtySchemaError,
  CompatibilityError,
  ErrCodeLeaseLost,
  ErrCodeQueueNotFound,
  ErrCodeValidation,
} from './errors';

// Protocol compatibility metadata.
export { SUPPORTED_PROTOCOL_MAJORS } from './protocol';

// Schema migrations (compatible with the Go client and the CLI).
export { migrate, getMigrationStatus, MigrationStatus } from './migrate';

// Import implementation classes
import { Connection as ConnectionImpl } from './connection';
import { ConnectionOptions } from './types';

/**
 * Create and connect a new PostgreMQ client
 * @param options - Connection options
 * @returns A connected client
 */
export async function connect(options: ConnectionOptions = {}): Promise<ConnectionImpl> {
  const client = new ConnectionImpl(options);
  try {
    await client.connect();
  } catch (err) {
    // Close what the connection created (its own pool, when it owns one).
    await client.close().catch(() => {});
    throw err;
  }
  return client;
}

// Create default exports
const PostgreMQ = {
  connect
};

export default PostgreMQ; 