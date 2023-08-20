const { createReadStream, createWriteStream } = require("fs");
const { Transform, pipeline } = require("stream");
const csv = require("csvtojson");
const jsc = require("json-to-csv-stream");
const util = require("util");

/**
 * ETL Transformer to extract certain fields from a csv and return the output as a csv.
 *
 * Uses the builder pattern from user input to dictate the stream sequence.
 *
 * Uses the strategy pattern to add the stream object based on the above input.
 *
 * User can pass in the stage options as the parameter to add stage.
 */
class ETLTransformer {
  constructor({ inputFile, outputFile, fields = [] , options = {} }) {
    this.inputFile = inputFile;
    this.outputFile = outputFile;
    this.fields = fields;
    this.stages = [];
    this.options = {
      combinator:",",
      defaultReplacer: "NULL",
      ...options
    }
  }
  async parseFile() {
    if (this.stages.length == 0) {
      throw new Error("Transformation Streams are not defined");
    }
    const asyncPipeline = util.promisify(pipeline);
    await asyncPipeline(...this.stages);
  }
  /**
   * @param {"input" | "output" | "transform" | "csvToJson" | "jsonToCsv"} stage {StageOptions}
   */
  addStage(stage) {
    switch (stage) {
      case "input":
        const inputStream = createReadStream(this.inputFile, {
          encoding: "utf8",
        });
        this.stages.push(inputStream);
        return this;
      case "transform":
        const fields = this.fields;
        const fieldSelector = this.fieldSelector;
        const options = this.options
        const transformStream = new Transform({
          transform(data, _, cb) {
            try {
              let inputData = {}
              let parsedData = JSON.parse(Buffer.from(data).toString())
              for(let [key,value] of Object.entries(parsedData)){
                try{
                  inputData[[key]] = JSON.parse(JSON.stringify(value))
                }catch(error){
                  inputData[[key]] = value
                }
              }
              const value = fieldSelector(inputData,fields,options);
              const outputData = `${JSON.stringify(value)}`;
              cb(null, outputData);
            } catch (err) {
              cb(err);
            }
          },
        });
        this.stages.push(transformStream);
        return this;

      case "csvToJson":
        const csvToJson = csv();
        this.stages.push(csvToJson);
        return this;

      case "jsonToCsv":
        const jsonToCsv = jsc();
        this.stages.push(jsonToCsv);
        return this;

      case "output":
        const outputStream = createWriteStream(this.outputFile, {
          encoding: "utf8",
        });
        this.stages.push(outputStream);
        return this;
    }
  }

  fieldSelector(inputData, fields,options) {
    if (fields.length == 0) {
      return inputData;
    } else {
      let returnData = {}
      fields.forEach((field) => {
        const { parent , child = [] , nested = false } = field
        if(!Array.isArray(child)){
          throw new Error("Child field has to be an array")
        }
        try{
          let parentVal="",childVal=""
          if(nested && child){
            parentVal = inputData[[parent]]
            parentVal = parentVal ? parentVal : "{}"
            childVal = JSON.parse(parentVal)
            child.forEach((ch)=>{
              returnData[ch] = Array.isArray(childVal) ? childVal.map(cVal => cVal[[ch]]).join(options.combinator) : childVal;
            })
            return
          }
          else{
            childVal = inputData[[parent]] ? inputData[[parent]] : options.defaultReplacer
            returnData[[parent]] = childVal
            return
          }
          
        }catch(error){
          throw new Error("Cannot Parse Parent Val or Child Val" + error.message)
        }

      });
      return returnData;
    }
  }
}

module.exports = {
  ETLTransformer,
};
