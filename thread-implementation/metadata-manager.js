import fs from "fs/promises";
import Logger from "./logger.js";
import { ensureDataDir } from "./utils.js";
import { LOG_DIRECTORY } from "./config.js";
import { ADMIN_COMMAND_TYPES, KAFKA_CLIENTS } from "./contansts.js";

const logger = new Logger(KAFKA_CLIENTS.ADMIN, false);

class KafkaAdminClient {
  constructor() {
    this.cache = {};
    this.metaWalPath = LOG_DIRECTORY.METADATA;
    ensureDataDir(this.metaWalPath);
  }

  async initialize() {
    await this.replayWAL();
  }

  replayWAL = async () => {
    logger.log("Loading metadata cache from", this.metaWalPath);
    const data = await fs.readFile(this.metaWalPath, "utf-8");
    const lines = data.split("\n").filter((line) => line.trim() !== "");
    for (const line of lines) {
      try {
        const record = JSON.parse(line);
        console.log("Replaying record:", record);
        this.applyRecord(record);
      } catch (err) {
        logger.error("Failed to parse WAL line:", line, err);
      }
    }
    logger.log("Metadata cache loaded:", this.cache);
  };

  createTopic = async (request) => {
    const { topic, partitions = 1, connectionId } = request;
    if (this.cache[topic]) {
      logger.log(`Topic ${topic} already exists`);
      return;
    }
    const record = {
      type: ADMIN_COMMAND_TYPES.CREATE_TOPIC,
      createdBy: connectionId,
      timestamp: Date.now(),
      name: topic,
      partitions,
    };
    await fs.appendFile(this.metaWalPath, JSON.stringify(record) + "\n");
    this.applyRecord(record);
    logger.log(`Topic ${topic} created with ${partitions} partitions`);
  };

  editTopicPartition = async (request) => {
    const { topic, newPartitionCount, connectionId } = request;
    if (!this.cache[topic]) {
      throw new Error(`Topic ${topic} does not exist`);
    }
    if(!newPartitionCount || isNaN(newPartitionCount)) {
      throw new Error(`Invalid new partition count: ${newPartitionCount}`);
    }
    if (newPartitionCount <= this.cache[topic].partitions) {
      throw new Error(`New partition count must be greater than existing count (${this.cache[topic].partitions})`);
    }
    const record = {
      type: ADMIN_COMMAND_TYPES.EDIT_PARTITIONS,
      createdBy: connectionId,
      timestamp: Date.now(),
      name: topic,
      partitions: newPartitionCount,
    };
    await fs.appendFile(this.metaWalPath, JSON.stringify(record) + "\n");
    this.applyRecord(record);
    logger.log(`Topic ${topic} partitions updated to ${newPartitionCount}`);
  };

  applyRecord = (record) => {
    switch (record.type) {
      case ADMIN_COMMAND_TYPES.CREATE_TOPIC || ADMIN_COMMAND_TYPES.EDIT_PARTITIONS:
        this.cache[record.name] = {
          partitions: record.partitions,
          createdBy: record.createdBy,
          createdAt: new Date(record.timestamp).toLocaleString(),
        };
        break;
      default:
        logger.log("Unknown admin record type:", record.type);
    }
  };

  listTopics = () => {
    return Object.keys(this.cache).map((topic) => ({
      name: topic,
      partitions: this.cache[topic].partitions,
      createdBy: this.cache[topic].createdBy || null,
      createdAt: this.cache[topic].timestamp || null,
    }));
  }

  checkTopicExists = (topic) => {
    return !!this.cache[topic];
  }
}

export default KafkaAdminClient;
