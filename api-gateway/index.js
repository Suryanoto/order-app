const http = require('http');
const redis = require('redis');

const client = redis.createClient({
  socket: { host: 'redis', port: 6379 }
});

client.on('error', err => console.log('Redis error:', err));

function parseBody(req) {
  return new Promise((resolve) => {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
      try { resolve(JSON.parse(body)); }
      catch { resolve({}); }
    });
  });
}

function send(res, status, data) {
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(data, null, 2));
}

async function handleRequest(req, res) {
  const { method, url } = req;
  console.log(`[${new Date().toISOString()}] ${method} ${url}`);

  await client.incr('total_requests');

  // Health check
  if (method === 'GET' && url === '/health') {
    const requests = await client.get('total_requests');
    return send(res, 200, {
      status: 'ok',
      service: 'api-gateway',
      total_requests: requests,
      timestamp: new Date().toISOString()
    });
  }

  // Get all orders
  if (method === 'GET' && url === '/orders') {
    const cached = await client.get('all_orders');
    if (cached) {
      console.log('⚡ Orders from cache');
      return send(res, 200, { source: 'cache', orders: JSON.parse(cached) });
    }
    return send(res, 200, { source: 'live', orders: [] });
  }

  // Get single order
  const orderMatch = url.match(/^\/orders\/(\w+)$/);
  if (method === 'GET' && orderMatch) {
    const order = await client.get(`order:${orderMatch[1]}`);
    if (order) {
      console.log('⚡ Order from cache');
      return send(res, 200, JSON.parse(order));
    }
    return send(res, 404, { error: 'Order not found' });
  }

  // Create new order
  if (method === 'POST' && url === '/orders') {
    const body = await parseBody(req);
    if (!body.product || !body.quantity || !body.price) {
      return send(res, 400, { error: 'product, quantity and price are required' });
    }

    const order = {
      id: 'order_' + Date.now(),
      product: body.product,
      quantity: body.quantity,
      price: body.price,
      status: 'pending',
      createdAt: new Date().toISOString()
    };

    // Cache the order in Redis
    await client.setEx(`order:${order.id}`, 3600, JSON.stringify(order));

    // Add to orders list
    const existing = await client.get('all_orders');
    const orders = existing ? JSON.parse(existing) : [];
    orders.push(order);
    await client.setEx('all_orders', 3600, JSON.stringify(orders));

    // Publish to Kafka via Redis pub/sub
    await client.publish('new_orders', JSON.stringify(order));

    console.log(`✅ Order created: ${order.id}`);
    return send(res, 201, order);
  }

  // Get all users
  if (method === 'GET' && url === '/users') {
    const users = await client.get('all_users');
    if (users) {
      return send(res, 200, JSON.parse(users));
    }
    return send(res, 200, []);
  }

  // Health of all services
  if (method === 'GET' && url === '/status') {
    const requests = await client.get('total_requests');
    const orders = await client.get('all_orders');
    const parsedOrders = orders ? JSON.parse(orders) : [];
    return send(res, 200, {
      gateway: 'online',
      redis: 'connected',
      total_requests: requests,
      total_orders: parsedOrders.length,
      services: {
        'api-gateway': 'online',
        'order-service': 'online',
        'notification-service': 'online',
        'payment-service': 'online',
        'user-service': 'online'
      }
    });
  }

  return send(res, 404, { error: `Route ${method} ${url} not found` });
}

async function main() {
  await client.connect();
  console.log('Redis connected ✅');

  const server = http.createServer(handleRequest);
  server.listen(3000, () => console.log('API Gateway running on port 3000 ✅'));
}

main();
