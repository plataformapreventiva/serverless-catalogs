Pub-Publication Catalog service - serverless-lambda
===================================

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
`serverless deploy -v`

## Deploy the Function
`serverless deploy function -f hello`

## Cleanup
`serverless remove`

[lambda]: https://aws.amazon.com/lambda/
[serverless]: https://serverless.com/ 
