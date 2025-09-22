# S3 Connection Troubleshooting Guide

## Error: "Failed to load data from S3 partitions"

This error occurs when the system cannot connect to S3 or access the partitioned data. Let's troubleshoot this step by step.

## Step 1: Check AWS Credentials

### Verify Environment Variables
First, check if your AWS credentials are properly set in your `.env` file:

```bash
# Check your .env file contains:
AWS_ACCESS_KEY_ID=your-access-key-here
AWS_SECRET_ACCESS_KEY=your-secret-key-here
AWS_DEFAULT_REGION=us-east-1
AWS_S3_BUCKET=your-bucket-name
```

### Test Credentials Manually
Run this Python script to test your credentials:

```python
import boto3
import os
from dotenv import load_dotenv

load_dotenv()

# Test credentials
try:
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    )
    
    # Test connection
    response = s3_client.list_buckets()
    print("✅ AWS credentials are working!")
    print(f"Available buckets: {[bucket['Name'] for bucket in response['Buckets']]}")
    
    # Test specific bucket access
    bucket_name = os.getenv('AWS_S3_BUCKET')
    if bucket_name:
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            print(f"✅ Can access bucket: {bucket_name}")
        except Exception as e:
            print(f"❌ Cannot access bucket {bucket_name}: {e}")
            
except Exception as e:
    print(f"❌ AWS credentials failed: {e}")
```

## Step 2: Check S3 Bucket Configuration

### Verify Bucket Name
Make sure your bucket name is correct in the `.env` file:

```bash
AWS_S3_BUCKET=your-actual-bucket-name
```

### Check Bucket Permissions
Your AWS credentials need these permissions:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::your-bucket-name",
                "arn:aws:s3:::your-bucket-name/*"
            ]
        }
    ]
}
```

### Test Bucket Access
```python
import boto3

s3_client = boto3.client('s3')
bucket_name = 'your-bucket-name'

try:
    # Test bucket access
    s3_client.head_bucket(Bucket=bucket_name)
    print(f"✅ Bucket {bucket_name} is accessible")
    
    # List objects in bucket
    response = s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=10)
    if 'Contents' in response:
        print("✅ Bucket has objects:")
        for obj in response['Contents'][:5]:
            print(f"  - {obj['Key']}")
    else:
        print("⚠️  Bucket is empty or no objects found")
        
except Exception as e:
    print(f"❌ Bucket access failed: {e}")
```

## Step 3: Check Partition Database

### Verify Database Exists
```bash
ls -la core/data/partition_navigation.db
```

### Check Database Content
```python
import sqlite3
import pandas as pd

conn = sqlite3.connect('core/data/partition_navigation.db')

# Check if partitions table exists
try:
    partitions_df = pd.read_sql_query("SELECT COUNT(*) as count FROM partitions", conn)
    print(f"✅ Database has {partitions_df['count'].iloc[0]} partitions")
    
    # Show sample partitions
    sample_df = pd.read_sql_query("SELECT * FROM partitions LIMIT 5", conn)
    print("Sample partitions:")
    print(sample_df[['payer_slug', 'state', 'billing_class', 's3_bucket', 's3_key']])
    
except Exception as e:
    print(f"❌ Database issue: {e}")
finally:
    conn.close()
```

## Step 4: Test S3 Partition Access

### Check Specific Partition Files
```python
import boto3
import sqlite3
import pandas as pd

# Get partition info from database
conn = sqlite3.connect('core/data/partition_navigation.db')
partitions_df = pd.read_sql_query("SELECT s3_bucket, s3_key FROM partitions LIMIT 5", conn)
conn.close()

# Test S3 access
s3_client = boto3.client('s3')

for _, row in partitions_df.iterrows():
    bucket = row['s3_bucket']
    key = row['s3_key']
    
    try:
        # Check if file exists
        s3_client.head_object(Bucket=bucket, Key=key)
        print(f"✅ Found: s3://{bucket}/{key}")
        
        # Test file size
        response = s3_client.head_object(Bucket=bucket, Key=key)
        size_mb = response['ContentLength'] / 1024 / 1024
        print(f"   Size: {size_mb:.2f} MB")
        
    except Exception as e:
        print(f"❌ Missing: s3://{bucket}/{key}")
        print(f"   Error: {e}")
```

## Step 5: Check Network Connectivity

### Test Internet Connection
```bash
# Test basic connectivity
ping google.com

# Test AWS endpoints
ping s3.amazonaws.com
```

### Check Firewall/Proxy
If you're behind a corporate firewall:
- Ensure HTTPS (port 443) is allowed
- Check if AWS S3 endpoints are whitelisted
- Verify proxy settings if applicable

## Step 6: Enable Debug Logging

Add this to your Django settings to see detailed S3 connection logs:

```python
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
        },
        'file': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'filename': 'logs/django.log',
        },
    },
    'loggers': {
        'core.utils.partition_navigator': {
            'level': 'DEBUG',
            'handlers': ['console', 'file'],
        },
        'boto3': {
            'level': 'DEBUG',
            'handlers': ['console', 'file'],
        },
        'botocore': {
            'level': 'DEBUG',
            'handlers': ['console', 'file'],
        },
    },
}
```

## Step 7: Common Solutions

### Solution 1: Use AWS CLI Configuration
If environment variables aren't working, try AWS CLI:

```bash
aws configure
# Enter your credentials when prompted
```

### Solution 2: Use IAM Roles (Production)
For production, use IAM roles instead of access keys:

```python
# In your Django settings
# Remove AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
# The system will automatically use IAM roles
```

### Solution 3: Check Region Mismatch
Ensure your S3 bucket region matches your AWS_DEFAULT_REGION:

```bash
aws s3api get-bucket-location --bucket your-bucket-name
```

### Solution 4: Verify Partition Paths
Check if your S3 paths in the database are correct:

```python
# The paths should look like:
# s3://your-bucket/partitions/payer=payer1/state=GA/billing_class=professional/data.parquet
```

## Step 8: Quick Fix Script

Run this comprehensive test script:

```python
import os
import boto3
import sqlite3
import pandas as pd
from dotenv import load_dotenv

def test_s3_connection():
    load_dotenv()
    
    print("🔍 Testing S3 Connection...")
    
    # Check environment variables
    aws_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    bucket_name = os.getenv('AWS_S3_BUCKET')
    
    print(f"✅ AWS Key: {'Set' if aws_key else 'Missing'}")
    print(f"✅ AWS Secret: {'Set' if aws_secret else 'Missing'}")
    print(f"✅ AWS Region: {aws_region}")
    print(f"✅ S3 Bucket: {bucket_name}")
    
    if not all([aws_key, aws_secret, bucket_name]):
        print("❌ Missing required environment variables!")
        return False
    
    # Test S3 connection
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_key,
            aws_secret_access_key=aws_secret,
            region_name=aws_region
        )
        
        # Test bucket access
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"✅ Bucket {bucket_name} is accessible")
        
        # Test database
        if os.path.exists('core/data/partition_navigation.db'):
            conn = sqlite3.connect('core/data/partition_navigation.db')
            count = pd.read_sql_query("SELECT COUNT(*) as count FROM partitions", conn).iloc[0]['count']
            print(f"✅ Database has {count} partitions")
            conn.close()
        else:
            print("❌ Partition database not found!")
            return False
            
        print("🎉 S3 connection test passed!")
        return True
        
    except Exception as e:
        print(f"❌ S3 connection failed: {e}")
        return False

if __name__ == "__main__":
    test_s3_connection()
```

## Still Having Issues?

1. **Check Django logs**: Look in `logs/django.log` for detailed error messages
2. **Verify bucket permissions**: Make sure your AWS user has the right permissions
3. **Test with AWS CLI**: Try `aws s3 ls s3://your-bucket-name`
4. **Contact your AWS administrator**: If using corporate AWS, they may need to grant permissions

The most common issues are:
- Missing or incorrect AWS credentials
- Wrong bucket name
- Insufficient S3 permissions
- Network connectivity issues
- Missing partition database

Run through these steps systematically to identify and fix the issue!
