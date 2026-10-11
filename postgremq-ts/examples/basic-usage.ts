/**
 * PostgreMQ TypeScript Client
 * Basic Usage Example
 */

import { connect } from '../src';

async function main() {
  // Connect to PostgreSQL
  const client = await connect({
    connectionString:
      process.env.DATABASE_URL ??
      'postgresql://postgres:postgres@localhost:5432/postgres',
    // Or specify connection details individually:
    // host: 'localhost',
    // port: 5432,
    // user: 'postgres',
    // password: 'postgres',
    // database: 'postgres',
  });

  try {
    console.log('Connected to PostgreSQL');

    // Create a topic and queue
    const topicName = 'example-topic';
    const queueName = 'example-queue';

    console.log(`Creating topic: ${topicName}`);
    await client.createTopic(topicName);

    console.log(`Creating queue: ${queueName} subscribing to ${topicName}`);
    await client.createQueue(queueName, topicName, false, {
      maxDeliveryAttempts: 3
    });

    // Publish a few messages
    console.log('Publishing messages...');
    for (let i = 1; i <= 5; i++) {
      const payload = { index: i, text: `This is message ${i}`, timestamp: new Date() };
      const messageId = await client.publish(topicName, payload);
      console.log(`Published message ${i} with ID: ${messageId}`);
    }

    // Create a consumer
    console.log(`\nCreating consumer for queue: ${queueName}`);
    const consumer = client.consume(queueName, {
      batchSize: 2,
      visibilityTimeoutSec: 30,
      autoExtension: {
        enabled: true,
        extensionThreshold: 0.5,
        extensionSec: 30
      }
    });

    // Consume messages
    console.log('Starting consumption (will process 5 messages):');
    let count = 0;
    
    // Process messages using for-await loop
    for await (const message of consumer.messages()) {
      try {
        console.log(`\nReceived message #${++count}:`);
        console.log(`  ID: ${message.id}`);
        console.log(`  Payload: ${JSON.stringify(message.payload)}`);
        console.log(`  Delivery Attempts: ${message.deliveryAttempts}`);
        console.log(`  Published At: ${message.publishedAt}`);
        console.log(`  Visibility Timeout: ${message.vt}`);

        // Simulate processing time
        const processingTime = Math.random() * 2000 + 1000;
        console.log(`  Processing for ${Math.round(processingTime)}ms...`);
        await new Promise(resolve => setTimeout(resolve, processingTime));

        // Randomly decide to ack, nack, or release
        const decision = Math.random();
        if (decision < 0.7) {
          // 70% chance to acknowledge
          console.log('  Acknowledging message');
          await message.ack();
        } else if (decision < 0.9) {
          // 20% chance to negatively acknowledge with delay
          const delaySeconds = 5;
          console.log(`  Negatively acknowledging message (retry after ${delaySeconds}s)`);
          await message.nack({ delaySeconds });
        } else {
          // 10% chance to release
          console.log('  Releasing message');
          await message.release();
        }
      } catch (error) {
        console.error('Error processing message:', error);
        
        // If there was an error processing, nack the message
        try {
          await message.nack({ delaySeconds: 10 });
        } catch (nackError) {
          console.error('Error nacking message:', nackError);
        }
      }

      // Stop after 5 messages
      if (count >= 5) {
        console.log('\nStopping consumer after 5 messages');
        await consumer.stop();
        break;
      }
    }

    // Show statistics
    console.log('\nQueue statistics:');
    const stats = await client.getQueueStatistics(queueName);
    console.log(`  Pending: ${stats.pendingCount}`);
    console.log(`  Processing: ${stats.processingCount}`);
    console.log(`  Completed: ${stats.completedCount}`);
    console.log(`  Total: ${stats.totalCount}`);

    // List all messages in the queue
    console.log('\nMessages in queue:');
    const messages = await client.listMessages(queueName);
    messages.forEach(msg => {
      console.log(`  ID: ${msg.messageId}, Status: ${msg.status}, Attempts: ${msg.deliveryAttempts}`);
    });

    // Clean up if needed
    const shouldCleanUp = true;
    if (shouldCleanUp) {
      console.log('\nCleaning up resources...');
      await client.cleanUpQueue(queueName);
      await client.deleteQueue(queueName);
      await client.cleanUpTopic(topicName);
      await client.deleteTopic(topicName);
      console.log('Clean up complete');
    }
  } catch (error) {
    console.error('Error:', error);
  } finally {
    // Close the connection
    console.log('\nClosing connection');
    await client.close();
    console.log('Connection closed');
  }
}

// Run the example
main().catch(error => {
  console.error('Uncaught error:', error);
  process.exit(1);
}); 