# =============================================================================
# deploy_fraud_pipeline.ps1
#
# Creates ALL AWS resources for the Fraud Detection Pipeline from scratch.
# Pipelines: A1 (per-event SQS), A2 (micro-batch SQS), B0 (batch S3/EventBridge)
#
# PREREQUISITES (run once before this script):
#   1. Install AWS CLI: https://aws.amazon.com/cli/
#   2. Configure credentials: aws configure
#      (enter Access Key ID, Secret Access Key, region, output=json)
#
# USAGE:
#   .\deploy_fraud_pipeline.ps1
# =============================================================================

# -----------------------------------------------------------------------------
# CONFIGURATION -- Edit these values before running
# -----------------------------------------------------------------------------
$REGION         = "us-east-1"
$BUCKET         = "fraud-detection-minhajul99"   # Must be globally unique
$TABLE_NAME     = "fraud-results"
$QUEUE_A1       = "fraud-queue-a1"
$QUEUE_A2       = "fraud-queue-a2"
$ROLE_NAME      = "fraud-lambda-role"

$FUNC_A1        = "fraud-pipeline-a1"
$FUNC_A2        = "fraud-pipeline-a2"
$FUNC_B0        = "fraud-pipeline-b0"

# Local file paths -- update to match your folder
$MODEL_LOCAL    = ".\fraud_model.pkl"
$FEATURES_LOCAL = ".\feature_columns.json"
$LAMBDA_A1_FILE = ".\lambda_a1.py"
$LAMBDA_A2_FILE = ".\lambda_a2.py"
$LAMBDA_B0_FILE = ".\batch_b0.py"

# Paste your Layer ARN here after running build_layer.sh in AWS CloudShell
# Example: arn:aws:lambda:us-east-1:123456789012:layer:fraud-ml-deps:1
$LAYER_ARN      = ""

# -----------------------------------------------------------------------------
# HELPER FUNCTIONS
# -----------------------------------------------------------------------------
function Write-Step($num, $msg) {
    Write-Host ""
    Write-Host "============================================================" -ForegroundColor Cyan
    Write-Host "  STEP $num -- $msg" -ForegroundColor Cyan
    Write-Host "============================================================" -ForegroundColor Cyan
}

function Write-OK($msg)   { Write-Host "  [OK]   $msg" -ForegroundColor Green  }
function Write-SKIP($msg) { Write-Host "  [SKIP] $msg" -ForegroundColor Yellow }
function Write-ERR($msg)  { Write-Host "  [ERR]  $msg" -ForegroundColor Red    }
function Write-INFO($msg) { Write-Host "  [INFO] $msg" -ForegroundColor White  }

function Get-AccountId {
    $id = (aws sts get-caller-identity --query Account --output text 2>$null)
    return $id.Trim()
}

# -----------------------------------------------------------------------------
# STEP 1 -- Validate AWS CLI and credentials
# -----------------------------------------------------------------------------
Write-Step 1 "Validating AWS CLI and credentials"

aws --version 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-ERR "AWS CLI not found. Install from https://aws.amazon.com/cli/"
    exit 1
}
Write-OK "AWS CLI found"

$ACCOUNT_ID = Get-AccountId
if (-not $ACCOUNT_ID) {
    Write-ERR "AWS credentials not configured. Run: aws configure"
    exit 1
}
Write-OK "Account ID : $ACCOUNT_ID"
Write-OK "Region     : $REGION"

# -----------------------------------------------------------------------------
# STEP 2 -- Create S3 Bucket and folder structure
# -----------------------------------------------------------------------------
Write-Step 2 "Creating S3 Bucket: $BUCKET"

aws s3api head-bucket --bucket $BUCKET 2>&1 | Out-Null
if ($LASTEXITCODE -eq 0) {
    Write-SKIP "Bucket '$BUCKET' already exists"
} else {
    if ($REGION -eq "us-east-1") {
        aws s3api create-bucket --bucket $BUCKET --region $REGION | Out-Null
    } else {
        aws s3api create-bucket --bucket $BUCKET --region $REGION `
            --create-bucket-configuration LocationConstraint=$REGION | Out-Null
    }
    if ($LASTEXITCODE -eq 0) {
        Write-OK "Bucket '$BUCKET' created"
    } else {
        Write-ERR "Failed to create bucket. The name may already be taken globally."
        Write-ERR "Edit the BUCKET variable at the top of this script and retry."
        exit 1
    }
}

aws s3api put-public-access-block --bucket $BUCKET `
    --public-access-block-configuration `
    "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true" | Out-Null
Write-OK "Public access blocked"

foreach ($folder in @("model/", "incoming/", "processed/", "sqs-payloads/", "layers/")) {
    aws s3api put-object --bucket $BUCKET --key $folder --content-length 0 | Out-Null
}
Write-OK "Folders created: model/ incoming/ processed/ sqs-payloads/ layers/"

# -----------------------------------------------------------------------------
# STEP 3 -- Upload model files to S3
# -----------------------------------------------------------------------------
Write-Step 3 "Uploading model files to S3"

if (Test-Path $MODEL_LOCAL) {
    aws s3 cp $MODEL_LOCAL "s3://$BUCKET/model/fraud_model.pkl" | Out-Null
    Write-OK "Uploaded fraud_model.pkl -> s3://$BUCKET/model/fraud_model.pkl"
} else {
    Write-SKIP "fraud_model.pkl not found at '$MODEL_LOCAL' -- upload manually later"
}

if (Test-Path $FEATURES_LOCAL) {
    aws s3 cp $FEATURES_LOCAL "s3://$BUCKET/model/feature_columns.json" | Out-Null
    Write-OK "Uploaded feature_columns.json -> s3://$BUCKET/model/feature_columns.json"
} else {
    Write-SKIP "feature_columns.json not found at '$FEATURES_LOCAL' -- upload manually later"
}

# -----------------------------------------------------------------------------
# STEP 4 -- Create DynamoDB Table
# -----------------------------------------------------------------------------
Write-Step 4 "Creating DynamoDB table: $TABLE_NAME"

aws dynamodb describe-table --table-name $TABLE_NAME --region $REGION 2>&1 | Out-Null
if ($LASTEXITCODE -eq 0) {
    Write-SKIP "Table '$TABLE_NAME' already exists"
} else {
    aws dynamodb create-table `
        --table-name $TABLE_NAME `
        --attribute-definitions AttributeName=TransactionID,AttributeType=S `
        --key-schema AttributeName=TransactionID,KeyType=HASH `
        --billing-mode PAY_PER_REQUEST `
        --region $REGION | Out-Null

    if ($LASTEXITCODE -eq 0) {
        Write-OK "DynamoDB table '$TABLE_NAME' created (on-demand billing)"
    } else {
        Write-ERR "Failed to create DynamoDB table"
        exit 1
    }
}

# -----------------------------------------------------------------------------
# STEP 5 -- Create SQS Queues
# -----------------------------------------------------------------------------
Write-Step 5 "Creating SQS Queues"

$sqsAttribs = "VisibilityTimeout=90,MessageRetentionPeriod=86400,ReceiveMessageWaitTimeSeconds=20"

foreach ($queueName in @($QUEUE_A1, $QUEUE_A2)) {
    $queueUrl = (aws sqs get-queue-url --queue-name $queueName --region $REGION `
        --query QueueUrl --output text 2>$null)
    if ($queueUrl -and $LASTEXITCODE -eq 0) {
        Write-SKIP "Queue '$queueName' already exists"
    } else {
        $result = (aws sqs create-queue `
            --queue-name $queueName `
            --attributes $sqsAttribs `
            --region $REGION `
            --query QueueUrl --output text 2>&1)
        if ($LASTEXITCODE -eq 0) {
            Write-OK "Queue '$queueName' created"
        } else {
            Write-ERR "Failed to create queue '$queueName': $result"
        }
    }
}

$QUEUE_A1_URL = (aws sqs get-queue-url --queue-name $QUEUE_A1 --region $REGION `
    --query QueueUrl --output text)
$QUEUE_A2_URL = (aws sqs get-queue-url --queue-name $QUEUE_A2 --region $REGION `
    --query QueueUrl --output text)

$QUEUE_A1_ARN = (aws sqs get-queue-attributes --queue-url $QUEUE_A1_URL `
    --attribute-names QueueArn --query "Attributes.QueueArn" --output text)
$QUEUE_A2_ARN = (aws sqs get-queue-attributes --queue-url $QUEUE_A2_URL `
    --attribute-names QueueArn --query "Attributes.QueueArn" --output text)

Write-OK "Queue A1 ARN: $QUEUE_A1_ARN"
Write-OK "Queue A2 ARN: $QUEUE_A2_ARN"

# -----------------------------------------------------------------------------
# STEP 6 -- Create IAM Role and Policies
# -----------------------------------------------------------------------------
Write-Step 6 "Creating IAM Role: $ROLE_NAME"

# Trust policy written to a temp file
$trustPolicyFile    = [System.IO.Path]::GetTempFileName() + ".json"
$trustPolicyContent = '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
[System.IO.File]::WriteAllText($trustPolicyFile, $trustPolicyContent)

aws iam get-role --role-name $ROLE_NAME 2>&1 | Out-Null
if ($LASTEXITCODE -eq 0) {
    Write-SKIP "Role '$ROLE_NAME' already exists"
} else {
    aws iam create-role `
        --role-name $ROLE_NAME `
        --assume-role-policy-document "file://$trustPolicyFile" | Out-Null
    Write-OK "IAM role '$ROLE_NAME' created"
}

aws iam attach-role-policy `
    --role-name $ROLE_NAME `
    --policy-arn "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole" | Out-Null
Write-OK "Attached AWSLambdaBasicExecutionRole (CloudWatch Logs)"

# Inline policy -- use -f format operator to safely embed variables into JSON strings
$sqsRes = ('"Resource":"arn:aws:sqs:{0}:{1}:fraud-queue-*"'        -f $REGION, $ACCOUNT_ID)
$s3Res  = ('"Resource":["arn:aws:s3:::{0}","arn:aws:s3:::{0}/*"]'  -f $BUCKET)
$ddRes  = ('"Resource":"arn:aws:dynamodb:{0}:{1}:table/{2}"'        -f $REGION, $ACCOUNT_ID, $TABLE_NAME)

$inlinePolicy = (
    '{"Version":"2012-10-17","Statement":[' +
    '{"Sid":"SQSAccess","Effect":"Allow","Action":["sqs:ReceiveMessage","sqs:DeleteMessage","sqs:GetQueueAttributes","sqs:SendMessage"],' + $sqsRes + '},' +
    '{"Sid":"S3Access","Effect":"Allow","Action":["s3:GetObject","s3:PutObject","s3:DeleteObject","s3:CopyObject","s3:ListBucket"],' + $s3Res + '},' +
    '{"Sid":"DynamoDBAccess","Effect":"Allow","Action":["dynamodb:PutItem","dynamodb:BatchWriteItem","dynamodb:GetItem"],' + $ddRes + '}' +
    ']}')

$inlinePolicyFile = [System.IO.Path]::GetTempFileName() + ".json"
[System.IO.File]::WriteAllText($inlinePolicyFile, $inlinePolicy)

aws iam put-role-policy `
    --role-name $ROLE_NAME `
    --policy-name "fraud-lambda-policy" `
    --policy-document "file://$inlinePolicyFile" | Out-Null
Write-OK "Inline policy attached (S3 + SQS + DynamoDB)"

$ROLE_ARN = (aws iam get-role --role-name $ROLE_NAME --query "Role.Arn" --output text)
Write-OK "Role ARN: $ROLE_ARN"

Remove-Item $trustPolicyFile  -ErrorAction SilentlyContinue
Remove-Item $inlinePolicyFile -ErrorAction SilentlyContinue

Write-INFO "Waiting 15 seconds for IAM role to propagate..."
Start-Sleep -Seconds 15

# -----------------------------------------------------------------------------
# STEP 7 -- Package Lambda ZIP files
# -----------------------------------------------------------------------------
Write-Step 7 "Packaging Lambda ZIP files"

function New-LambdaZip($sourceFile, $zipPath) {
    if (-not (Test-Path $sourceFile)) {
        Write-SKIP "Source not found: $sourceFile"
        return $false
    }
    if (Test-Path $zipPath) { Remove-Item $zipPath -Force }
    Compress-Archive -Path $sourceFile -DestinationPath $zipPath -Force
    Write-OK "Created $zipPath"
    return $true
}

$ZIP_A1 = ".\lambda_a1.zip"
$ZIP_A2 = ".\lambda_a2.zip"
$ZIP_B0 = ".\batch_b0.zip"

New-LambdaZip $LAMBDA_A1_FILE $ZIP_A1 | Out-Null
New-LambdaZip $LAMBDA_A2_FILE $ZIP_A2 | Out-Null
New-LambdaZip $LAMBDA_B0_FILE $ZIP_B0 | Out-Null

# -----------------------------------------------------------------------------
# STEP 8 -- Deploy Lambda Functions
# -----------------------------------------------------------------------------
Write-Step 8 "Deploying Lambda Functions"

$envVars = "Variables={MODEL_BUCKET=$BUCKET,MODEL_KEY=model/fraud_model.pkl,FEATURES_KEY=model/feature_columns.json,INCOMING_PREFIX=incoming/}"

function Deploy-Lambda($funcName, $zipFile, $handler, $timeoutSec, $description) {
    Write-INFO "Deploying $funcName (timeout=${timeoutSec}s)..."

    if (-not (Test-Path $zipFile)) {
        Write-SKIP "$funcName -- zip not found, skipping"
        return
    }

    aws lambda get-function --function-name $funcName --region $REGION 2>&1 | Out-Null

    if ($LASTEXITCODE -eq 0) {
        aws lambda update-function-code `
            --function-name $funcName `
            --zip-file "fileb://$zipFile" `
            --region $REGION | Out-Null
        Write-OK "$funcName code updated"
        Start-Sleep -Seconds 5

        aws lambda update-function-configuration `
            --function-name $funcName `
            --handler $handler `
            --runtime python3.12 `
            --role $ROLE_ARN `
            --timeout $timeoutSec `
            --memory-size 1024 `
            --environment $envVars `
            --region $REGION | Out-Null
        Write-OK "$funcName configuration updated"
    } else {
        aws lambda create-function `
            --function-name $funcName `
            --runtime python3.12 `
            --role $ROLE_ARN `
            --handler $handler `
            --zip-file "fileb://$zipFile" `
            --timeout $timeoutSec `
            --memory-size 1024 `
            --environment $envVars `
            --description $description `
            --region $REGION | Out-Null

        if ($LASTEXITCODE -eq 0) {
            Write-OK "$funcName created"
        } else {
            Write-ERR "Failed to create $funcName"
        }
    }
}

# A1 and A2: 60s timeout | B0: 300s timeout (scans all files in incoming/)
if (Test-Path $ZIP_A1) { Deploy-Lambda $FUNC_A1 $ZIP_A1 "lambda_a1.handler"  60  "Fraud A1 - per-event SQS" }
if (Test-Path $ZIP_A2) { Deploy-Lambda $FUNC_A2 $ZIP_A2 "lambda_a2.handler"  60  "Fraud A2 - micro-batch SQS" }
if (Test-Path $ZIP_B0) { Deploy-Lambda $FUNC_B0 $ZIP_B0 "batch_b0.handler"   300 "Fraud B0 - batch S3 scan" }

# -----------------------------------------------------------------------------
# STEP 9 -- Attach Lambda Layer (if ARN provided)
# -----------------------------------------------------------------------------
Write-Step 9 "Attaching Lambda Layer"

if ($LAYER_ARN -eq "") {
    Write-SKIP "LAYER_ARN is empty -- skipping layer attachment"
    Write-INFO "To attach the layer later, run:"
    Write-INFO "  .\attach_layer.ps1 -LayerArn YOUR_LAYER_ARN"
    Write-INFO ""
    Write-INFO "To build the layer (no Docker needed):"
    Write-INFO "  1. Open AWS CloudShell in the AWS Console browser"
    Write-INFO "  2. Run the commands from build_layer.sh"
    Write-INFO "  3. Copy the LayerVersionArn from the output"
    Write-INFO "  4. Run: .\attach_layer.ps1 -LayerArn PASTE_ARN_HERE"
} else {
    foreach ($funcName in @($FUNC_A1, $FUNC_A2, $FUNC_B0)) {
        aws lambda update-function-configuration `
            --function-name $funcName `
            --layers $LAYER_ARN `
            --region $REGION | Out-Null
        if ($LASTEXITCODE -eq 0) {
            Write-OK "Layer attached to $funcName"
        } else {
            Write-ERR "Failed to attach layer to $funcName"
        }
        Start-Sleep -Seconds 3
    }
}

# -----------------------------------------------------------------------------
# STEP 10 -- Create SQS to Lambda Event Source Mappings
# -----------------------------------------------------------------------------
Write-Step 10 "Creating SQS to Lambda triggers"

function Add-SqsTrigger($funcName, $queueArn, $batchSize) {
    $existing = (aws lambda list-event-source-mappings `
        --function-name $funcName `
        --event-source-arn $queueArn `
        --region $REGION `
        --query "EventSourceMappings[0].UUID" `
        --output text 2>$null)

    if ($existing -and $existing -ne "None") {
        Write-SKIP "SQS trigger already exists for $funcName (UUID: $existing)"
    } else {
        aws lambda create-event-source-mapping `
            --function-name $funcName `
            --event-source-arn $queueArn `
            --batch-size $batchSize `
            --enabled `
            --region $REGION | Out-Null
        if ($LASTEXITCODE -eq 0) {
            Write-OK "SQS trigger created for $funcName (batch-size=$batchSize)"
        } else {
            Write-ERR "Failed to create SQS trigger for $funcName"
        }
    }
}

Add-SqsTrigger $FUNC_A1 $QUEUE_A1_ARN 1    # A1: one message at a time
Add-SqsTrigger $FUNC_A2 $QUEUE_A2_ARN 10   # A2: up to 10 messages per batch

# -----------------------------------------------------------------------------
# STEP 11 -- Create EventBridge Rule for B0 (every 1 minute)
# -----------------------------------------------------------------------------
Write-Step 11 "Creating EventBridge rule for B0"

$RULE_NAME   = "fraud-b0-every-minute"
$FUNC_B0_ARN = (aws lambda get-function --function-name $FUNC_B0 --region $REGION `
    --query "Configuration.FunctionArn" --output text)

aws events put-rule `
    --name $RULE_NAME `
    --schedule-expression "rate(1 minute)" `
    --state ENABLED `
    --description "Triggers fraud B0 batch scoring every minute" `
    --region $REGION | Out-Null
Write-OK "EventBridge rule '$RULE_NAME' created"

aws lambda add-permission `
    --function-name $FUNC_B0 `
    --statement-id "allow-eventbridge-b0" `
    --action "lambda:InvokeFunction" `
    --principal "events.amazonaws.com" `
    --source-arn "arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${RULE_NAME}" `
    --region $REGION 2>$null | Out-Null
Write-OK "EventBridge permission granted on Lambda B0"

$targetJson = "[{`"Id`":`"fraud-b0-target`",`"Arn`":`"$FUNC_B0_ARN`"}]"
aws events put-targets `
    --rule $RULE_NAME `
    --targets $targetJson `
    --region $REGION | Out-Null
Write-OK "B0 Lambda set as EventBridge target"

# -----------------------------------------------------------------------------
# STEP 12 -- Summary
# -----------------------------------------------------------------------------
Write-Host ""
Write-Host "============================================================" -ForegroundColor Green
Write-Host "  DEPLOYMENT COMPLETE" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
Write-Host "  S3 Bucket  : $BUCKET"                                       -ForegroundColor Green
Write-Host "  DynamoDB   : $TABLE_NAME"                                    -ForegroundColor Green
Write-Host "  Queue A1   : $QUEUE_A1_URL"                                  -ForegroundColor Green
Write-Host "  Queue A2   : $QUEUE_A2_URL"                                  -ForegroundColor Green
Write-Host "  Lambda A1  : $FUNC_A1  (60s / 1024MB)"                      -ForegroundColor Green
Write-Host "  Lambda A2  : $FUNC_A2  (60s / 1024MB)"                      -ForegroundColor Green
Write-Host "  Lambda B0  : $FUNC_B0  (300s / 1024MB)"                     -ForegroundColor Green
Write-Host "  IAM Role   : $ROLE_ARN"                                      -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green

if ($LAYER_ARN -eq "") {
    Write-Host ""
    Write-Host "  [!] LAYER NOT ATTACHED YET" -ForegroundColor Yellow
    Write-Host "  Run build_layer.sh in AWS CloudShell, then:" -ForegroundColor Yellow
    Write-Host "  .\attach_layer.ps1 -LayerArn YOUR_LAYER_ARN" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "  NEXT STEPS:" -ForegroundColor Cyan
Write-Host "  1. Build the Lambda Layer in AWS CloudShell (build_layer.sh)"  -ForegroundColor Cyan
Write-Host "  2. Run: .\attach_layer.ps1 -LayerArn YOUR_LAYER_ARN"           -ForegroundColor Cyan
Write-Host "  3. Run: python producer.py --csv your_data.csv --pipeline all"  -ForegroundColor Cyan
Write-Host "  4. Check DynamoDB '$TABLE_NAME' for scored results"             -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Green
