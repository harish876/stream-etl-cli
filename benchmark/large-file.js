const { performance } = require("perf_hooks");
const { ETLTransformer } = require("../index");

async function run() {
  const start = performance.now();

  console.log("File parsing started...");

  const transformer = new ETLTransformer({
    inputFile: "files/large-file-result.csv",
    outputFile: "files/large-file-result1.csv",
  });

  transformer
    .addStage("input")
    .addStage("csvToJson")
    .addStage("transform")
    .addStage("jsonToCsv")
    .addStage("output");

  await transformer.parseFile().catch(err => console.log(err.message));

  const end = performance.now();
  console.log("File parsing completed...");

  const timeTaken = end - start;
  console.log(`Duration : ${Math.round(timeTaken / 1000)} s`);
}

run();
