const AWS = require("aws-sdk");
const dynamodb = new AWS.DynamoDB.DocumentClient();
const { rechandler }= require('./maxRec.js')
const { savhandler }= require('./savingsestimate.js')


exports.handler = async (event) => {
let jobName = await event.detail.jobName;
let jobTracker = []

//Pull job name from tracker

    let tparams = {
        TableName: 'ETLJobTracker'
    }

    const dynamodbTagPromise = dynamodb.scan(tparams).promise();

    await  dynamodbTagPromise.then((data)=> {
        jobTracker = data.Items
    }).catch((err)=>{
        console.log(err)
    })

   for (let i=0;i<jobTracker.length;i++) {
       if (jobTracker[i].JobName == jobName) {
           let estimateinputs = await rechandler({jobName})
           let status = await savhandler(estimateinputs)
           console.log(status)
       } else {
           console.log(`${jobTracker[i].JobName} is not currently a tracked job`)
       }
   }

};
