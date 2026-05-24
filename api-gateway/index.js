const http = require('http');
const redis = require('redis');

const client = redis.createClient({
  socket: { host: 'redis', port: 6379 }
});

client.on('error', err => console.log('Redis error:', err));

function route(req, res) {
  const { method, url } = req;
  console.log(`[${method}] ${url}`);

  // Health check
  if (url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', service: 'api-gateway' }));
    return;
  }

  // Orders route
  if (url.startsWith('/orders')) {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      service: 'order-service',
      message: 'Order request received',
      timestamp: new Date().toISOString()
    }));
    return;
  }

  // Users route
  if (url.startsWith('/users')) {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      service: 'user-service',
      message: 'User request received',
      timestamp: new Date().toISOString()
    }));
    return;
  }

  // Payments route
  if (url.startsWith('/payments')) {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      service: 'payment-service',
      message: 'Payment request received',
      timestamp: new Date().toISOString()
    }));
    return;
  }

  // 404
  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ error: 'Route not found' }));
}

async function main() {
  await client.connect();
  console.log('Redis connected ✅');

  const server = http.createServer(async (req, res) => {
    // Track requests in Redis
    await client.incr('total_requests');
    const count = await client.get('total_requests');
    res.setHeader('X-Request-Count', count);
    route(req, res);
  });

  server.listen(3000, () => console.log('API Gateway running on port 3000 ✅'));
}

main();
