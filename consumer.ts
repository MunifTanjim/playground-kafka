import { Kafka } from "kafkajs";

const topic = "test-topic";

const kafka = new Kafka({
  clientId: "playground",
  brokers: ["127.0.0.1:9094"],
});

const consumers = [
  kafka.consumer({
    groupId: "playground-consumer",
    allowAutoTopicCreation: true,
  }),
  kafka.consumer({
    groupId: "playground-consumer",
    allowAutoTopicCreation: true,
  }),
];

await Promise.all(consumers.map((consumer) => consumer.connect()));

await Promise.all(
  consumers.map((consumer) =>
    consumer.subscribe({
      topic,
      fromBeginning: true,
    })
  )
);

await Promise.all(
  consumers.map((consumer, idx) =>
    consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log(`Consumer ${idx}`, {
          topic,
          partition,
          "message.value": message.value?.toString(),
        });
      },
    })
  )
);

const errorTypes = ["unhandledRejection", "uncaughtException"];
const signalTraps = ["SIGTERM", "SIGINT", "SIGUSR2"];

errorTypes.forEach((type) => {
  process.on(type, async (e) => {
    try {
      console.log(`process.on ${type}`);
      console.error(e);
      await Promise.all(consumers.map((consumer) => consumer.disconnect()));
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
      await Promise.all(consumers.map((consumer) => consumer.disconnect()));
    } finally {
      process.kill(process.pid, type);
    }
  });
});
