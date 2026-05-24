const { Kafka } = require('kafkajs');
const redis = require('redis');

const kafka = new Kafka({
  clientId: 'order-service',
  brokers: ['kafka:9092']
});

const producer = kafka.producer();

const subscriber = redis.createClient({
  socket: { host: 'redis', port: 6379 }
});

const store = redis.createClient({
  socket: { host: 'redis', port: 6379 }
});

async function processOrder(order) {
  console.log(`📦 Processing order: ${order.id}`);

  // Update status to processing
  order.status = 'processing';
  await store.setEx(`order:${order.id}`, 3600, JSON.stringify(order));

  // Send to Kafka for other services
  await producer.send({
    topic: 'orders',
    messages: [{ key: order.id, value: JSON.stringify(order) }]
  });

  console.log(`✅ Order ${order.id} sent to Kafka`);
}

async function main() {
  await subscriber.connect();
  await store.connect();
  await producer.connect();
  console.log('Order Service started ✅');

  // Listen for new orders from API Gateway via Redis pub/sub
  await subscriber.subscribe('new_orders', async (message) => {
    const order = JSON.parse(message);
    await processOrder(order);
  });

  console.log('Listening for new orders... 👂');
}

main();
