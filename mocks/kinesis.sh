#!/bin/bash
awslocal kinesis create-stream --stream-name input-stream --shard-count 1 --cli-connect-timeout 6000 --region us-east-1
