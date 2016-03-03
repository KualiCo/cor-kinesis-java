# Core Kinesis Consumer

This is sample Java code that consumes messages from an Amazon Kinesis stream provided by Kuali, logs it to a file.  It is intended that you will change that part of the code to save changes to a database you use or perform some other action when you get messages from Kuali.

Also, the client state information is saved to Amazon DynamoDB, which is necessary to keep in sync.  

## Prerequisites

- Install git
- Install Java 1.7 or higher
- Get an AWS Access Key ID and Secret Access Key from Kuali.  This will have the privileges you need to read from the Kinesis stream, write to DynamoDB to save state information for the client, and push events to CloudWatch (for monitoring).

## Installation

Clone the repo
```
git clone https://github.com/KualiCo/core-kinesis-java.git
```

Clone the amazon-kinesis-client repo
```
git clone https://github.com/awslabs/amazon-kinesis-client.git
```

Modify the app.properties file to work in your environment by changing the streamName and applicationName to cor-kinesis-`<region>`-`<environment>`-`<application>`-`<institution>` where:
- `<region>` = AWS region - saas1 (Oregon), saas2 (Ireland)
- `<environment>` = environment of application - tst, sbx, stg, or prd
- `<application>` = abbreviated name of the application - stu-cm, res-coi, etc.
- `<institution>` = url name of your institution - monsters, byu, coventry, etc.
```
vim credentials
```

In com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration, specify the region in one of the following ways:
- Use the "withRegionName" method
- Use the complete constructor, though it needs a couple dozen arguments (the default values for the constructor can all be found in the provided code)

Make changes to the code so it will not only log messages, but also update make changes to databases and take other actions.

## Use

Run by building with ant and then running.

With it running, as you make changes in your Kuali application (e.g., Curriculum Management, Kuali Research, etc.), those changes will be logged to the log file.  They will also be written to databases and and other actions that you have defined in the code will be performed.

## Message Format

Messages will be in JSON and look similar to those listed below.

When it is an insert, old_val will be null:

```
{  
   "0":null,
   "1":{  
      "new_val":{  
         "created":1456503966314,
         "createdBy":"1281228650558160821",
         "id":"36f7dd5a-05a3-4f27-b9d7-2126e0a8b78e",
         "meta":{  
            "proposalType":"create"
         },
         "pid":"Eyk6svYil",
         "status":"draft",
         "updated":1456503967380
      },
      "old_val":null
   },
   "id":"cc7a4d49-639f-5d95-0f6e-cf03c9880163",
   "tableName":"courses",
   "institution":"monsters",
   "environment":"stg"
}
```

When it is an update, old_val and new_val will not be null:

```
{  
   "0":null,
   "1":{  
      "new_val":{  
         "created":1456503966314,
         "createdBy":"1281228650558160821",
         "id":"36f7dd5a-05a3-4f27-b9d7-2126e0a8b78e",
         "meta":{  
            "proposalType":"create"
         },
         "pid":"Eyk6svYil",
         "status":"draft",
         "updated":1456503967380
      },
      "old_val":{  
         "created":1456503966314,
         "createdBy":"1281228650558160821",
         "id":"36f7dd5a-05a3-4f27-b9d7-2126e0a8b78e",
         "meta":{  
            "proposalType":"create"
         },
         "pid":"Eyk6svYil",
         "status":"draft"
      }
   },
   "id":"a88fc106-c2f3-6b84-805a-e040fecf54a3",
   "tableName":"courses",
   "institution":"monsters",
   "environment":"stg"
}
```

When it is a delete, new_val will be null:

```
{  
   "0":null,
   "1":{  
      "new_val":null,
      "old_val":{  
         "created":1456434468850,
         "createdBy":"1281228650558160821",
         "description":"test",
         "id":"a6855169-b829-49cb-9c53-5409e2dd9eb2",
         "meta":{  
            "proposalType":"create"
         },
         "pid":"EyBrh8uol",
         "proposalRationale":"this will be a great class",
         "startTerm":{  
            "year":"2016"
         },
         "status":"draft",
         "transcriptTitle":"test",
         "updated":1456434544370
      }
   },
   "id":"3684bc95-d505-1a07-539d-e29ff1525b94",
   "tableName":"courses",
   "institution":"monsters",
   "environment":"stg"
}
```

Possible tables for CM are:
- actionlists
- actionlogs
- config
- courses (includes course proposals)
- definitions
- experiences
- files
- groups
- instances
- logbot
- options
- pgroups
- programs (includes program proposals)
- specializations

## See also

* https://github.com/awslabs/amazon-kinesis-client
* https://github.com/aws/aws-sdk-java/tree/master/src/samples/AmazonKinesis
* http://docs.aws.amazon.com/kinesis/latest/dev/developing-consumers-with-kcl.html
* http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-record-processor-implementation-app-java.html
