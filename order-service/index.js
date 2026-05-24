const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'order-service',
  brokers: ['kafka:9092']
});

const producer = kafka.producer();

const orders = [];

async function createOrder(product, quantity, price) {
  const order = {
    id: 'order_' + Date.now(),
    product,
    quantity,
    price,
    status: 'pending',
    createdAt: new Date().toISOString()
  };

  orders.push(order);

  await producer.send({
    topic: 'orders',
    messages: [{ key: order.id, value: JSON.stringify(order) }]
  });

  console.log(`📦 Order created and sent to Kafka:`, order);
  return order;
}

async function main() {
  await producer.connect();
  console.log('Order Service started ✅');

  // Simulate orders coming in
  await createOrder('Laptop', 1, 999);
  await new Promise(r => setTimeout(r, 2000));
  await createOrder('Phone', 2, 599);
  await new Promise(r => setTimeout(r, 2000));
  await createOrder('Keyboard', 1, 149);

  console.log(`Total orders processed: ${orders.length}`);
  await producer.disconnect();
}

main();
