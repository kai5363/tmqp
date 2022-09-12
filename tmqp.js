/* eslint-disable max-classes-per-file */
const net = require('net');
const EventEmitter = require('node:events');
const crypto = require('crypto');

class MyEmitter extends EventEmitter {}
const http = require('http');

const randomId = () => crypto.randomBytes(8).toString('hex');
const myEmitter = new MyEmitter();

function stringToHostAndPort(address) {
  return { host: address.split(':')[0], port: address.split(':')[1] };
}

async function getMasterIp(turtlekeeperConfig) {
  return new Promise((resolve) => {
    http.get(turtlekeeperConfig, (res) => {
      res.on('data', (data) => {
        const ip = data.toString();
        const config = stringToHostAndPort(ip);
        resolve(config);
      });
    }).end();
  });
}

class Connection {
  constructor(client) {
    this.client = client;
    this.consumers = [];
  }

  async produce(queue, messages, option) {
    return new Promise((resolve, reject) => {
      const produceObj = {
        id: randomId(),
        method: 'produce',
        maxLength: option?.maxLength,
        queue,
        messages: typeof messages === 'string' ? [messages] : [...messages],
      };
      this.send(produceObj);
      console.log(`${JSON.stringify(produceObj)}`);
      myEmitter.on(produceObj.id, (data) => {
        console.log(`produce: ${JSON.stringify(data)}`);
        if (data.success) {
          resolve(data.message);
        }
        reject(data.message);
      });
    });
  }

  async consume(queue, nums = 1) {
    return new Promise((resolve) => {
      const consumeObj = {
        id: randomId(),
        method: 'consume',
        queue,
        nums,
      };
      this.send(consumeObj);
      console.log(`${JSON.stringify(consumeObj)}`);
      myEmitter.on(consumeObj.id, (data) => {
        resolve(data.messages);
      });
    });
  }

  send(messages) {
    this.client.write(`${JSON.stringify(messages)}\r\n\r\n`);
  }

  end() {
    this.client.end();
  }
}

class tmqp {
  constructor(config) {
    this.config = config;
  }

  async connect() {
    return new Promise((resolve, reject) => {
      const client = net.connect(this.config);

      client.on('connect', () => {
        console.log('connected to server!');
        client.on('readable', () => {
          let reqBuffer = Buffer.from('');
          let buf;
          let reqHeader;
          while (true) {
            buf = client.read();
            if (buf === null) break;

            reqBuffer = Buffer.concat([reqBuffer, buf]);

            // Indicating end of a request
            const marker = reqBuffer.indexOf('\r\n\r\n');
            if (marker !== -1) {
              // Record the data after \r\n\r\n
              const remaining = reqBuffer.slice(marker + 4);
              reqHeader = reqBuffer.slice(0, marker).toString();
              // Push the extra readed data back to the socket's readable stream
              client.unshift(remaining);
              break;
            }
          }

          if (!reqHeader) return;
          const object = JSON.parse(reqHeader);

          if (object.message === 'connected') {
            resolve(new Connection(client));
          } else if (object.method === 'consume') {
            myEmitter.emit(object.id, object);
          } else if (object.method === 'produce') {
            myEmitter.emit(object.id, object);
          }
        });
      });

      client.on('error', () => {
        console.log('Oops! cannot connect to the server');
        reject(new Error('cannot connect to the server'));
      });
      client.on('end', () => {
        console.log('disconnected from server');
      });
    });
  }

  async connectTurtlekeeper() {
    const config = await getMasterIp(this.config);
    return new Promise((resolve) => {
      const client = net.connect(config);

      client.on('connect', () => {
        console.log('connected to server!');
        client.on('readable', () => {
          let reqBuffer = Buffer.from('');
          let buf;
          let reqHeader;
          while (true) {
            buf = client.read();
            if (buf === null) break;

            reqBuffer = Buffer.concat([reqBuffer, buf]);

            // Indicating end of a request
            const marker = reqBuffer.indexOf('\r\n\r\n');
            if (marker !== -1) {
              // Record the data after \r\n\r\n
              const remaining = reqBuffer.slice(marker + 4);
              reqHeader = reqBuffer.slice(0, marker).toString();
              // Push the extra readed data back to the socket's readable stream
              client.unshift(remaining);
              break;
            }
          }

          if (!reqHeader) return;
          const object = JSON.parse(reqHeader);

          if (object.message === 'connected') {
            this.connection = new Connection(client);
            resolve(this.connection);
          } else if (object.method === 'consume') {
            myEmitter.emit(object.id, object);
          } else if (object.method === 'produce') {
            myEmitter.emit(object.id, object);
          }
        });
      });

      client.on('error', () => {
        console.log('Oops! cannot connect to the server');
        setTimeout(async () => {
          const connection = await this.connectTurtlekeeper(this.config);
          resolve(connection);
          // reject(new Error('cannot connect to the server'));
        }, 5000);
      });
      client.on('end', () => {
        console.log('disconnected from server');
      });
    });
  }
}

module.exports = tmqp;
