import { createConnection } from 'net';

const socket = createConnection({ port: 9092 }, () => {
  console.log('Connected to broker!');
  
  // Send a valid JSON message with newline delimiter
  const message = {
    type: 'produce',
    topic: 'test-topic',
    partition: 0,
    value: 'Hello from client!'
  };
  
  socket.write(JSON.stringify(message) + '\n');
});

socket.on('data', (data) => {
  console.log('Response from broker:', data.toString());
});

socket.on('end', () => {
  console.log('Disconnected from broker');
});

socket.on('error', (err) => {
  console.error('Connection error:', err.message);
});