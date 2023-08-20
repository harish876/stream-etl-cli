const { performance } = require("perf_hooks");
const { ETLTransformer } = require("../index");

async function run() {
  const start = performance.now();

  console.log("File parsing started...");

  const transformer = new ETLTransformer({
    inputFile: "files/work-example.csv",
    outputFile: "files/work-example-result.csv",
    fields: [
      { parent: "salesOrderId"},
      { parent: "studentDetails", child: ["studentId","studentName"] , nested: true }
    ],
  });

  transformer
    .addStage("input")
    .addStage("csvToJson")
    .addStage("transform")
    .addStage("jsonToCsv")
    .addStage("output");

  await transformer.parseFile().catch(console.error);

  const end = performance.now();
  console.log("File parsing completed...");

  const timeTaken = end - start;
  console.log(`Duration : ${Math.round(timeTaken / 1000)} s`);
}

run();