const axios = require('axios');
const { createWriteStream } = require('fs');
const { Readable } = require('stream');
const { pipeline } = require('stream/promises');

async function createAxiosResponseStream(url) {
  const response = await axios.get(url, { responseType: 'stream' });

  const readableStream = new Readable({
    read() {
      response.data.on('data', (chunk) => {
        this.push(chunk);
      });

      response.data.on('end', () => {
        this.push(null);
      });

      response.data.on('error', (error) => {
        this.emit('error', error);
      });
    },
  });

  return readableStream;
}

class AxiosResponseStream extends Readable {
  constructor(url, options) {
    super(options);
    this.url = url;
    this.initialize();
  }

  initialize() {
    axios.get(this.url, { responseType: 'stream' })
      .then((response) => {
        this.responseStream = response.data;
        this.responseStream.on('data', (chunk) => {
          if (!this.push(chunk)) {
            this.responseStream.pause();
          }
        });

        this.responseStream.on('end', () => {
          this.push(null);
        });

        this.responseStream.on('error', (error) => {
          this.emit('error', error);
        });

        this.responseStream.resume();
      })
      .catch((error) => {
        this.emit('error', error);
      });
  }

  _read() {
    if (this.responseStream) {
      this.responseStream.resume();
    }
  }
}

async function start(){
  const apiUrl = 'https://jsonplaceholder.typicode.com/photos';

  // const axiosResponseStream = await createAxiosResponseStream(apiUrl);
  const axiosResponseStream = new AxiosResponseStream(apiUrl);

  const writeStream = createWriteStream("./files/axios.json")
  
  return new Promise((resolve,reject) => {
    pipeline(
      axiosResponseStream,
      writeStream
    )
    
    axiosResponseStream.on('data', (chunk) => {
      console.log('Received a chunk of data:', chunk.length, 'bytes');
    });
    
    axiosResponseStream.on('end', () => {
      console.log('Finished reading the response data.');
      resolve()
    });
    
    axiosResponseStream.on('error', (error) => {
      console.error('Error reading the response data:', error);
      reject(error)
    });
  })
}


start()
