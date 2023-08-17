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
  constructor({ inputFile, outputFile, fields = [] }) {
    this.inputFile = inputFile;
    this.outputFile = outputFile;
    this.fields = fields;
    this.stages = [];
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
        const transformStream = new Transform({
          transform(data, _, cb) {
            try {
              const inputData = JSON.parse(data);
              const value = fieldSelector(inputData, fields);
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

  fieldSelector(inputData, fields) {
    if (fields.length == 0) {
      return inputData;
    } else {
      const tmp = {
        ...inputData,
      };
      fields.forEach((field) => {});
      return inputData;
    }
  }
}

module.exports = {
  ETLTransformer,
};
