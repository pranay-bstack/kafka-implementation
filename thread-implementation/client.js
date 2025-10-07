import net from "net";
import readline from "readline";
import { API_TYPES, KAFKA_CLIENTS } from "./contansts.js";

class KafkaClient {
  constructor(host = "localhost", port = 9092) {
    this.host = host;
    this.port = port;
    this.socket = null;
    this.connected = false;
  }

  connect() {
    return new Promise((resolve, reject) => {
      this.socket = net.createConnection({ host: this.host, port: this.port });

      this.socket.on("connect", () => {
        console.log("✓ Connected to Kafka broker");
        this.connected = true;
        this.setupResponseHandler();
        resolve();
      });

      this.socket.on("error", (err) => {
        console.error("✗ Connection error:", err.message);
        reject(err);
      });

      this.socket.on("end", () => {
        console.log("Connection closed");
        this.connected = false;
      });
    });
  }

  setupResponseHandler() {
    let buffer = Buffer.alloc(0);

    this.socket.on("data", (chunk) => {
      buffer = Buffer.concat([buffer, chunk]);

      let newlineIndex;
      while ((newlineIndex = buffer.indexOf("\n")) !== -1) {
        const line = buffer.slice(0, newlineIndex);
        buffer = buffer.slice(newlineIndex + 1);

        try {
          const response = JSON.parse(line.toString());
          console.log("\n📥 Response:", JSON.stringify(response, null, 2));
        } catch (err) {
          console.error("Failed to parse response:", err.message);
        }
      }
    });
  }

  send(request) {
    if (!this.connected) {
      throw new Error("Not connected to broker");
    }
    this.socket.write(JSON.stringify(request) + "\n");
    console.log("Sent:", JSON.stringify(request, null, 2));
  }

  // Admin API
  createTopic(name, partitions = 1) {
    this.send({
      apiKey: API_TYPES.CREATE_TOPIC,
      topic: name,
      partitions,
    });
  }

  editPartitions(topic, newPartitionCount) {
    this.send({
      apiKey: API_TYPES.EDIT_PARTITIONS,
      topic,
      newPartitionCount,
    });
  }

  listTopics() {
    this.send({
      apiKey: API_TYPES.LIST_TOPICS,
    });
  }

  // Producer API
  produce(topic, value, partition = 0, key = null) {
    this.send({
      apiKey: API_TYPES.PRODUCE,
      topic,
      partition,
      key,
      value,
    });
  }

  close() {
    if (this.socket) {
      this.socket.end();
    }
  }
}

// Interactive CLI
async function startCLI() {
  const client = new KafkaClient();

  try {
    await client.connect();
  } catch (err) {
    console.error("Failed to connect:", err.message);
    process.exit(1);
  }

  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    prompt: "kafka> ",
  });

  console.log("\n=== Kafka Client CLI ===");
  console.log("Commands:");
  console.log("  create-topic <name> [partitions]");
  console.log("  edit-partitions <topic> <count>");
  console.log("  list-topics");
  console.log("  produce <topic> <value> [partition] [key]");
  console.log("  exit");
  console.log("========================\n");

  rl.prompt();

  rl.on("line", (line) => {
    const parts = line.trim().split(/\s+/);
    const command = parts[0];

    try {
      switch (command) {
        case "create-topic":
          if (parts.length < 2) {
            console.log("Usage: create-topic <name> [partitions]");
          } else {
            const name = parts[1];
            const partitions = parseInt(parts[2]) || 1;
            client.createTopic(name, partitions);
          }
          break;

        case "edit-partitions":
          if (parts.length < 3) {
            console.log("Usage: edit-partitions <topic> <count>");
          } else {
            const topic = parts[1];
            const count = parseInt(parts[2]);
            client.editPartitions(topic, count);
          }
          break;

        case "list-topics":
          client.listTopics();
          break;

        case "produce":
          if (parts.length < 3) {
            console.log("Usage: produce <topic> <value> [partition] [key]");
          } else {
            const topic = parts[1];
            const value = parts[2];
            const partition = parseInt(parts[3]) || 0;
            const key = parts[4] || null;
            client.produce(topic, value, partition, key);
          }
          break;

        case "exit":
        case "quit":
          console.log("Goodbye!");
          client.close();
          process.exit(0);
          break;

        case "help":
          console.log("\nCommands:");
          console.log("  create-topic <name> [partitions]");
          console.log("  edit-partitions <topic> <count>");
          console.log("  list-topics");
          console.log("  produce <topic> <value> [partition] [key]");
          console.log("  exit\n");
          break;

        default:
          if (command) {
            console.log(`Unknown command: ${command}. Type 'help' for commands.`);
          }
      }
    } catch (err) {
      console.error("Error:", err.message);
    }

    rl.prompt();
  });

  rl.on("close", () => {
    console.log("\nGoodbye!");
    client.close();
    process.exit(0);
  });
}

// Command line argument support
if (process.argv.length > 2) {
  // Non-interactive mode
  const client = new KafkaClient();

  client.connect().then(() => {
    const command = process.argv[2];
    const args = process.argv.slice(3);

    switch (command) {
      case "create-topic":
        client.createTopic(args[0], parseInt(args[1]) || 1);
        break;
      case "edit-partitions":
        client.editPartitions(args[0], parseInt(args[1]));
        break;
      case "produce":
        client.produce(args[0], args[1], parseInt(args[2]) || 0, args[3] || null);
        break;
      default:
        console.log("Unknown command:", command);
    }

    setTimeout(() => {
      client.close();
      process.exit(0);
    }, 1000);
  });
} else {
  // Interactive mode
  startCLI();
}

export default KafkaClient;

// USAGE
// node client.js
// # Then type commands:
// kafka> create-topic my-topic 3
// kafka> produce my-topic "hello world" 0 key1
// kafka> edit-partitions my-topic 5
// kafka> exit
