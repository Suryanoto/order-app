const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'payment-service',
  brokers: ['kafka:9092']
});

const consumer = kafka.consumer({ groupId: 'payment-group' });
const producer = kafka.producer();

async function processPayment(order) {
  // Simulate payment processing
  await new Promise(r => setTimeout(r, 500));
  const success = Math.random() > 0.1; // 90% success rate

  const result = {
    orderId: order.id,
    status: success ? 'paid' : 'failed',
    amount: order.price * order.quantity,
    processedAt: new Date().toISOString()
  };

  // Send payment result back to Kafka
  await producer.send({
    topic: 'payments',
    messages: [{ key: order.id, value: JSON.stringify(result) }]
  });

  console.log(`💳 Payment ${result.status} for order ${order.id} — $${result.amount}`);
}

async function main() {
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
