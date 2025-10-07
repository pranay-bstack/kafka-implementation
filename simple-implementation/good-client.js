import { createConnection } from 'net';

class SimpleProducer {
  constructor(config = {}) {
    this.host = config.host || 'localhost';
    this.port = config.port || 9092;
    this.socket = null;
    this.connected = false;
    this.messageCount = 0;
    
    this.stats = {
      sent: 0,
      acked: 0,
      errors: 0,
      totalLatency: 0
    };
  }

  connect() {
    return new Promise((resolve, reject) => {
      this.socket = createConnection({
        host: this.host,
        port: this.port
      });

      this.socket.on('connect', () => {
        console.log(`Connected to broker at ${this.host}:${this.port}`);
        this.connected = true;
        this.setupResponseHandler();
        resolve();
      });

      this.socket.on('error', (err) => {
        console.error('Connection error:', err.message);
        this.connected = false;
        reject(err);
      });

      this.socket.on('close', () => {
        console.log('Connection closed');
        this.connected = false;
      });
    });
  }

  setupResponseHandler() {
    let buffer = Buffer.alloc(0);

    this.socket.on('data', (chunk) => {
      buffer = Buffer.concat([buffer, chunk]);

      // Parse newline-delimited JSON responses
      let newlineIndex;
      while ((newlineIndex = buffer.indexOf('\n')) !== -1) {
        const line = buffer.slice(0, newlineIndex);
        buffer = buffer.slice(newlineIndex + 1);

        try {
          const response = JSON.parse(line.toString());
          this.handleResponse(response);
        } catch (err) {
          console.error('Failed to parse response:', err.message);
        }
      }
    });
  }

  handleResponse(response) {
    if (response.status === 'success') {
      this.stats.acked++;
      this.stats.totalLatency += response.latency || 0;
      
      console.log(`✓ Message acknowledged:`, {
        offset: response.offset,
        topic: response.topic,
        latency: response.latency + 'ms'
      });
    } else {
      this.stats.errors++;
      console.error(`✗ Error response:`, response.message);
    }
  }

  async send(topic, value, key = null) {
    if (!this.connected) {
      throw new Error('Not connected to broker');
    }

    const message = {
      topic,
      key,
      value,
      timestamp: Date.now()
    };

    const data = JSON.stringify(message) + '\n';
    
    return new Promise((resolve, reject) => {
      this.socket.write(data, (err) => {
        if (err) {
          reject(err);
        } else {
          this.stats.sent++;
          resolve();
        }
      });
    });
  }

  getStats() {
    const avgLatency = this.stats.acked > 0 
      ? (this.stats.totalLatency / this.stats.acked).toFixed(2)
      : 0;

    return {
      ...this.stats,
      avgLatency: avgLatency + 'ms',
      successRate: this.stats.sent > 0
        ? ((this.stats.acked / this.stats.sent) * 100).toFixed(2) + '%'
        : '0%'
    };
  }

  close() {
    if (this.socket) {
      this.socket.end();
    }
  }
}

// Example usage
async function runTest() {
  const producer = new SimpleProducer({
    host: 'localhost',
    port: 9092
  });

  try {
    await producer.connect();

    console.log('\n=== Starting test: Sending 20 messages ===\n');

    // Send messages one by one
    for (let i = 0; i < 20; i++) {
      await producer.send(
        'test-topic',
        `Message number ${i} - ${new Date().toISOString()}`,
        `key-${i % 5}` // 5 different keys
      );
      
      // Small delay to see individual messages
      await new Promise(resolve => setTimeout(resolve, 100));
    }

    // Wait a bit for all responses
    await new Promise(resolve => setTimeout(resolve, 2000));

    console.log('\n=== Test Complete ===');
    console.log('Producer Stats:', producer.getStats());

  } catch (err) {
    console.error('Test failed:', err);
  } finally {
    producer.close();
  }
}

// Run test if executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  runTest();
}

export default SimpleProducer;