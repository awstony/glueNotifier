const AWS = require("aws-sdk");
const sns = new AWS.SNS();

//Independent Variables from Glue Job Run Metrics
let provisionedDPU = 100;
let maxExecutorsNeeded = 107;
let executionTime = 400;

//Value Store for Key Dependent Variables
let results = {
  maxRecCapacity:0,
  savings:0,
  provisioningFactor:0,
  savingsRate:0
};

//Recommended Capacity Logic
const maxRecommendedCapacity = (provDPU, maxExec) => {
  let maxAllocatedExecutors = (((provDPU-1)*2)-1);
  let provFactor = (maxExec+1)/(maxAllocatedExecutors+1);
  results.provisioningFactor = provFactor
  results.maxRecCapacity = Math.round(((provDPU-1)*provFactor)+1);
};

//Savings Estimate Logic
const jobSavingsEstimate = (prov, rec, execTime) => {
  let minimumExecutionTime = 600;
  let billTime = Math.max(execTime,minimumExecutionTime);
  const unitCost = (.44/3600)
  let provJobCost = (prov*billTime*unitCost);
  let recJobCost = (rec*billTime*unitCost);
  results.savings = (provJobCost-recJobCost).toFixed(2);
  results.savingsRate = (results.savings/provJobCost).toFixed(2);
}


exports.handler = async (event) => {

    //Implementation block
    maxRecommendedCapacity (provisionedDPU, maxExecutorsNeeded);
    jobSavingsEstimate(provisionedDPU, results.maxRecCapacity, executionTime);

    const sparams = {
      Message: `For job PLACEHOLDER, consider running ${results.maxRecCapacity} DPU's for potential savings of $${results.savings} or ${results.savingsRate} percent per Job!`,
      MessageAttributes: {
        'Set': {
          'DataType': 'String',
          'StringValue': 'String'
        }},
        TopicArn:'arn:aws:sns:us-east-1:012314179935:GlueOptimizerAlert'
    }

    const snsPromise = sns.publish(sparams).promise();

    await snsPromise.then((data)=> {
      console.log(data)
    }).catch((err)=>{
      console.log(err)
    });


   /*
    const response = {
      body: `For job PLACEHOLDER, consider running ${results.maxRecCapacity} DPU's for potential savings of $${results.savings} or ${results.savingsRate} percent per Job!`
    };

    return response.body;
    */
};
