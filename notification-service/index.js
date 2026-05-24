const { Kafka } = require('kafkajs');
const redis = require('redis');

const kafka = new Kafka({
  clientId: 'notification-service',
  brokers: ['kafka:9092']
});

const consumer = kafka.consumer({ groupId: 'notification-group' });

const store = redis.createClient({
  socket: { host: 'redis', port: 6379 }
});

async function sendNotification(order) {
  console.log('---------------------------');
  console.log(`📧 Notification for order ${order.id}`);
  console.log(`   Product:  ${order.product}`);
  console.log(`   Quantity: ${order.quantity}`);
  console.log(`   Price:    $${order.price}`);
  console.log(`   Status:   ${order.status}`);

  // Update order status in Redis
  order.status = 'notified';
  await store.setEx(`order:${order.id}`, 3600, JSON.stringify(order));

  // Track notifications count
  await store.incr('total_notifications');
  const total = await store.get('total_notifications');

  console.log(`✅ Notification sent! (Total: ${total})`);
}

async function main() {
  await store.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: 'orders', fromBeginning: false });
  console.log('Notification Service started ✅');

  await consumer.run({
    eachMessage: async ({ message }) => {
      const order = JSON.parse(message.value.toString());
      await sendNotification(order);
    }
  });
}

main();
