const{ createReadStream, createWriteStream } = require("fs");
const { Transform , pipeline } = require("stream")
const csv = require("csvtojson") 
const jsonToCsv = require('json-to-csv-stream')

function parseFile(input, output) {
  const inputStream = createReadStream(input, { encoding: "utf8" });
  const outputStream = createWriteStream(output, { encoding: "utf8" });

  const csvParser = csv({})
  const jsonParser = jsonToCsv({})
  const transformStream = new Transform({
    transform(data,_,cb){
        try{
            const input = JSON.parse(data)
            const colors = JSON.parse(input?.colors)
            const tmp = {
                ...input,
                colors: colors.map(c => c.color).join(',')
            }
            const output = `${JSON.stringify(tmp)}`
            cb(null,output)
        }catch(err){
            cb(err)
        }
    }
  })

  pipeline(inputStream,csvParser,transformStream,jsonParser,outputStream,err => {
    if(err){
        console.log("Error" + err)
    } else {
        console.log("Pipeline completed successfully")
    }
  })

}

parseFile("./files/example.csv", "./files/result.csv");
