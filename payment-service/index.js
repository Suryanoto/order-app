const { Kafka } = require('kafkajs');
const redis = require('redis');

const kafka = new Kafka({
  clientId: 'payment-service',
  brokers: ['kafka:9092']
});

const consumer = kafka.consumer({ groupId: 'payment-group' });
const producer = kafka.producer();

const store = redis.createClient({
  socket: { host: 'redis', port: 6379 }
});

async function processPayment(order) {
  console.log(`💳 Processing payment for order ${order.id}`);
  await new Promise(r => setTimeout(r, 500));

  const success = Math.random() > 0.1;
  const payment = {
    orderId: order.id,
    status: success ? 'paid' : 'failed',
    amount: order.price * order.quantity,
    processedAt: new Date().toISOString()
  };

  // Store payment result
  await store.setEx(`payment:${order.id}`, 3600, JSON.stringify(payment));

  // Update order status
  const existing = await store.get(`order:${order.id}`);
  if (existing) {
    const updatedOrder = JSON.parse(existing);
    updatedOrder.status = payment.status === 'paid' ? 'paid' : 'payment_failed';
    updatedOrder.payment = payment;
    await store.setEx(`order:${order.id}`, 3600, JSON.stringify(updatedOrder));
  }

  // Publish payment result to Kafka
  await producer.send({
    topic: 'payments',
    messages: [{ key: order.id, value: JSON.stringify(payment) }]
  });

  console.log(`${success ? '✅' : '❌'} Payment ${payment.status} — $${payment.amount}`);
}

async function main() {
  await store.connect();
  await consumer.connect();
  await producer.connect();
  await consumer.subscribe({ topic: 'orders', fromBeginning: false });
  console.log('Payment Service started ✅');

  await consumer.run({
    eachMessage: async ({ message }) => {
      const order = JSON.parse(message.value.toString());
      await processPayment(order);
    }
  });
}

main();
