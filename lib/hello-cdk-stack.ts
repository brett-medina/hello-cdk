import * as cdk from 'aws-cdk-lib';
import {
  aws_kinesis as kinesis,
  aws_dynamodb as dynamodb,
  aws_lambda as lambda,
  aws_s3 as s3 
} from 'aws-cdk-lib';
import { KinesisEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import * as kinesisanalytics from 'aws-cdk-lib/aws-kinesisanalyticsv2';
import { Construct } from 'constructs';
import * as path from 'path';
// import * as sqs from 'aws-cdk-lib/aws-sqs';

export class HelloCdkStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // The code that defines your stack goes here
    const stream = new kinesis.Stream(this, 'MyFirstStream', {
      streamName: 'input-stream',
    });

    const table = new dynamodb.Table(this, 'Table', {
      tableName: 'kinesisAggs',
      partitionKey: { name: 'vendorId', type: dynamodb.AttributeType.NUMBER },
    });

    const fn = new lambda.Function(this, 'MyFunction', {
      functionName: 'kinesisLambdaConsumer01',
      runtime: lambda.Runtime.PYTHON_3_9,
      handler: 'lambda_function.lambda_handler',
      environment: {
        dynamoDBTableName: table.tableName
      },
      code: lambda.Code.fromAsset(path.join(__dirname, 'backend/lambda')),
      timeout: cdk.Duration.minutes(1)
    });

    fn.addEventSource(new KinesisEventSource(stream, {
      startingPosition: lambda.StartingPosition.LATEST,
      batchSize: 1000,
      maxBatchingWindow: cdk.Duration.seconds(120),
      retryAttempts: 2,
      tumblingWindow: cdk.Duration.seconds(30) 
    }))

    table.grantWriteData(fn);

    const taxiDataBucket = new s3.Bucket(this, `nyctaxitrips-108545`, {
      removalPolicy: cdk.RemovalPolicy.RETAIN
    })

    const curatedDataBucket = new s3.Bucket(this, `curateddata-108545`, {
      removalPolicy: cdk.RemovalPolicy.RETAIN
    })

  }
}
