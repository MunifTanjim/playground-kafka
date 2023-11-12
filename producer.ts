import { Kafka } from "kafkajs";

process.env.KAFKAJS_NO_PARTITIONER_WARNING = "1";

const topic = "test-topic";

const kafka = new Kafka({
  clientId: "playground",
  brokers: ["127.0.0.1:9094"],
});

const producer = kafka.producer({
  allowAutoTopicCreation: true,
});

await producer.connect();

const message = {
  value: `${Date.now()} Hello KafkaJS user!`,
  partition: Math.floor(Math.random() * 2),
};

console.log("message", message);

const result = await producer.send({
  topic,
  messages: [message],
});

console.log("result", result);

const errorTypes = ["unhandledRejection", "uncaughtException"];
const signalTraps = ["SIGTERM", "SIGINT", "SIGUSR2"];

errorTypes.forEach((type) => {
  process.on(type, async () => {
    try {
      console.log(`process.on ${type}`);
      await producer.disconnect();
      process.exit(0);
    } catch (_) {
      process.exit(1);
    }
  });
});

signalTraps.forEach((type) => {
  process.once(type, async () => {
    try {
      console.log(`signal ${type}`);
      await producer.disconnect();
    } finally {
      process.kill(process.pid, type);
    }
  });
});

await producer.disconnect();
