const AWS = require("aws-sdk");
const sns = new AWS.SNS();

exports.savhandler = (event) => {

//Savings Estimate Logic
const jobSavingsEstimate = async (prov, rec, execTime, name) => {
  let minimumExecutionTime = 600;
  let billTime = Math.max(execTime,minimumExecutionTime);
  const unitCost = (.44/3600)
  let provJobCost = (prov*billTime*unitCost);
  let recJobCost = (rec*billTime*unitCost);
  let savings = (provJobCost-recJobCost).toFixed(2);
  let savingsRate = (savings/provJobCost).toFixed(2);

//Publish savings notification

  const sparams = {
    Message: `For job ${name}, consider running ${rec} DPU's for potential savings of $${savings} or ${savingsRate} percent per Job!`,
    MessageAttributes: {
      'Set': {
        'DataType': 'String',
        'StringValue': 'String'
      }},
      TopicArn:'arn:aws:sns:us-east-1:012314179935:GlueOptimizerAlert'
  }

  //Publish message
  const snsPromise = sns.publish(sparams).promise();

  await snsPromise.then((data)=> {
    console.log(data)
  }).catch((err)=>{
    console.log(err)
  });

 return `${name} Savings estimate published!`;

};

  return jobSavingsEstimate(event.provisionedDPU,event.maxReqCapacity,event.executionTime, event.JobName);
};
