const AWS = require("aws-sdk");
const sns = new AWS.SNS();
const glue = new AWS.Glue();
const cloudwatch = new AWS.CloudWatch();
const dynamodb = new AWS.DynamoDB.DocumentClient();


exports.rechandler = (event) => {

 //Recommended Capacity Logic
  const maxRecommendedCapacity = async (runJob) => {

    //get job details from Glue
     const gparams = {
       JobName: runJob,
       MaxResults: 5
     }

     const gluePromise = glue.getJobRuns(gparams).promise();

     const glueValues = await gluePromise.then((data)=> {
       let provisionedDPU = data.JobRuns[0].AllocatedCapacity;
       let executionTime = data.JobRuns[0].ExecutionTime;
       let runId = data.JobRuns[0].Id;

       return {
       provisionedDPU: provisionedDPU,
       executionTime: executionTime,
       runId: runId
     }
       //console.log(data)
     }).catch((err)=> {
       console.log(err)
     });

     //get max needed Executors from Cloudwatch
     const cparams = {
       EndTime: new Date('2020-07-09T17:10:00'),
       MetricName: 'glue.driver.ExecutorAllocationManager.executors.numberMaxNeededExecutors',
       Namespace: 'Glue',
       Period: 60,
       StartTime: new Date('2020-07-09T14:10:00'),
       Dimensions: [{
               Name: 'JobName',
               Value: runJob,
             },{
               Name: 'JobRunId',
               Value: 'ALL'
             },{
               Name: 'Type',
               Value: 'gauge'
             }],
       Statistics: ['Maximum']
     }

     //send message to recipients
     const cwPromise = cloudwatch.getMetricStatistics(cparams).promise();

     const cwValues = await cwPromise.then((data)=> {
      let maxExecutorsNeeded = data.Datapoints[0].Maximum

      return {
        maxExecutorsNeeded: maxExecutorsNeeded
      }
      //console.log(data)
     }).catch((err)=> {
       console.log(err)
     });

    let maxAllocatedExecutors = (((glueValues.provisionedDPU-1)*2)-1);
    let provFactor = (cwValues.maxExecutorsNeeded+1)/(maxAllocatedExecutors+1);
    let maxReqCapacity = Math.round(((glueValues.provisionedDPU-1)*provFactor)+1);

    let response = {
      maxReqCapacity: maxReqCapacity,
      executionTime: glueValues.executionTime,
      provisionedDPU: glueValues.provisionedDPU,
      JobName: runJob
    }

   return response;
};
 return maxRecommendedCapacity(event.jobName)
};
