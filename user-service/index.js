const redis = require('redis');
const http = require('http');

const client = redis.createClient({
  socket: { host: 'redis', port: 6379 }
});

const users = [
  { id: '1', name: 'Surya', email: 'surya@email.com', role: 'admin' },
  { id: '2', name: 'Alice', email: 'alice@email.com', role: 'user' },
  { id: '3', name: 'Bob',   email: 'bob@email.com',   role: 'user' }
];

async function main() {
  await client.connect();
  console.log('User Service started ✅');

  // Cache all users in Redis on startup
  for (const user of users) {
    await client.setEx(`user:${user.id}`, 3600, JSON.stringify(user));
  }
  console.log('Users cached in Redis ✅');

  const server = http.createServer(async (req, res) => {
    res.setHeader('Content-Type', 'application/json');

    if (req.url === '/users') {
      res.end(JSON.stringify(users));
      return;
    }

    const match = req.url.match(/^\/users\/(\w+)$/);
    if (match) {
      const cached = await client.get(`user:${match[1]}`);
      if (cached) {
        console.log(`⚡ User ${match[1]} served from cache`);
        res.end(cached);
      } else {
        res.writeHead(404);
        res.end(JSON.stringify({ error: 'User not found' }));
      }
      return;
    }

    res.writeHead(404);
    res.end(JSON.stringify({ error: 'Route not found' }));
  });

  server.listen(3001, () => console.log('User Service running on port 3001 ✅'));
}

main();
