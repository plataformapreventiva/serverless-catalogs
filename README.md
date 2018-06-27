Pub-Publication Catalog Service - Serverless-Lambda
===================================

Registry of Social Beneficiaries (PUB) Monthly Catalog Service Pipeline 



Requirements
------------

To run this, you will need:

* Python and a recent `pip`
* The AWS CLI (`pip install awscli`)
* Serverless framework (`npm install -g serverless`)
* Plugin existing-s3, overcomes the CloudFormation limitation (`npm install serverless-plugin-existing-s3`)

Usage
-----

## Setup serverless credentials
`serverless config credentials --provider aws --key $AWS_ACCESS_KEY_ID --secret $AWS_SECRET_ACCESS_KEY_ID`

## Deploy the Service
`sls deploy`

## Add S3 triggers to the Service
`sls s3deploy` 

## Cleanup
`sls remove`

TODO()
------------
- First lambda function to query Athena for monthly Pub Metadata Update
- Second lambda function to trigger elasticsearch update 
