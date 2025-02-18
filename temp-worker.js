
const { parentPort } = require('worker_threads');
const { getLatestLiveMessage } = require('C:\Users\sujan\OneDrive\Desktop\sarayu-web\Project\sarayu-node-backend\middlewares\mqttHandler.js');

// Process topic streams
parentPort.on('message', async ({ action, topics }) => {
  if (action === 'processBatch') {
    const results = [];
    
    for (const topic of topics) {
      try {
        const latestMessage = await getLatestLiveMessage(topic);
        if (latestMessage) {
          results.push({ topic, message: latestMessage });
        }
      } catch (error) {
        parentPort.postMessage({ error: `Error processing topic ${topic}: ${error.message}` });
      }
    }
    
    parentPort.postMessage({ results });
  }
});
