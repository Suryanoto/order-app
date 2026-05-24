const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'notification-service',
  brokers: ['kafka:9092']
});

const consumer = kafka.consumer({ groupId: 'notification-group' });

async function main() {
  await consumer.connect();
  await consumer.subscribe({ topic: 'orders', fromBeginning: false });
  console.log('Notification Service started ✅');
  console.log('Listening for orders... 👂');

  await consumer.run({
    eachMessage: async ({ message }) => {
      const order = JSON.parse(message.value.toString());
      console.log('---------------------------');
      console.log(`📧 Sending notification for order ${order.id}`);
      console.log(`   Product:  ${order.product}`);
      console.log(`   Quantity: ${order.quantity}`);
      console.log(`   Price:    $${order.price}`);
      console.log(`   Status:   ${order.status}`);
      console.log(`✅ Notification sent!`);
    }
  });
}

main();
