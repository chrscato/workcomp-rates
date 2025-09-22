# Dataset Review System - Setup Guide

## Overview

The Dataset Review System is your primary tool for exploring and analyzing partitioned healthcare data from S3. It combines multiple partitions into a unified dataframe and provides comprehensive insights into data quality, statistical summaries, and business intelligence.

## Features

- **S3 Integration**: Automatically combines multiple parquet partitions from S3
- **Comprehensive Analysis**: Data quality metrics, statistical summaries, and business insights
- **Interactive Interface**: Tabbed interface for different analysis views
- **Export Capabilities**: Export combined datasets as CSV or Parquet
- **Progress Tracking**: Real-time loading progress and performance metrics
- **Error Handling**: Robust error handling with detailed logging

## Setup Instructions

### 1. Environment Variables Configuration

Create a `.env` file in your project root with the following AWS credentials:

```bash
# AWS S3 Configuration
AWS_ACCESS_KEY_ID=your-aws-access-key-id
AWS_SECRET_ACCESS_KEY=your-aws-secret-access-key
AWS_DEFAULT_REGION=us-east-1
AWS_S3_BUCKET=your-partitioned-data-bucket
AWS_S3_PREFIX=partitions/

# Optional: AWS Session Token (for temporary credentials)
# AWS_SESSION_TOKEN=your-session-token

# Django Settings
SECRET_KEY=your-secret-key-here
DEBUG=True
```

### 2. AWS Credentials Setup

#### Option A: Environment Variables (Recommended for Development)
Set the environment variables in your `.env` file as shown above.

#### Option B: AWS CLI Configuration
```bash
aws configure
```

#### Option C: IAM Roles (Recommended for Production)
Attach an IAM role to your EC2 instance or use ECS task roles.

### 3. Required AWS Permissions

Your AWS credentials need the following S3 permissions:

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
                "arn:aws:s3:::your-partitioned-data-bucket",
                "arn:aws:s3:::your-partitioned-data-bucket/*"
            ]
        }
    ]
}
```

### 4. S3 Bucket Structure

Ensure your S3 bucket has the following structure:

```
your-partitioned-data-bucket/
├── partitions/
│   ├── payer=payer1/state=GA/billing_class=professional/year=2023/month=01/
│   │   └── data.parquet
│   ├── payer=payer1/state=GA/billing_class=professional/year=2023/month=02/
│   │   └── data.parquet
│   └── ...
```

### 5. Database Setup

Ensure your partition navigation database is properly set up:

```bash
# Create partition inventory (if not already done)
python s3_partition_inventory.py your-bucket-name --create-db
```

## Usage Guide

### 1. Accessing the Dataset Review

1. Navigate to the Commercial Rate Insights page
2. Select your required filters (Payer, State, Billing Class)
3. Click the "Review Dataset" button (the primary action button)
4. The system will automatically combine matching partitions from S3

### 2. Analysis Features

#### Dataset Summary Tab
- Total records and columns
- Memory usage
- Column type distribution
- Load performance metrics
- Data quality recommendations

#### Data Quality Tab
- Null value analysis
- Data type validation
- Unique value counts
- Sample values for each column

#### Statistics Tab
- Descriptive statistics for numeric columns
- Mean, median, standard deviation
- Min/max values and quartiles

#### Business Insights Tab
- Rate analysis and distribution
- Top payers and organizations
- Billing class breakdown
- Healthcare-specific metrics

#### Data Preview Tab
- First 100 records of the combined dataset
- Full column visibility
- Formatted values (e.g., currency formatting)

### 3. Export Options

- **CSV Export**: Download as comma-separated values
- **Parquet Export**: Download in optimized parquet format

### 4. Performance Considerations

- **Default Limit**: 50,000 rows (configurable via `max_rows` parameter)
- **Memory Usage**: Monitor memory consumption for large datasets
- **Load Time**: Progress tracking shows load performance
- **Caching**: Results are cached for improved performance

## Configuration Options

### 1. Adjusting Row Limits

Add `?max_rows=100000` to the URL to increase the row limit:

```
/commercial/insights/dataset-review/?payer_slug=payer1&state=GA&max_rows=100000
```

### 2. Analysis Types

- **Comprehensive** (default): Full analysis with all metrics
- **Basic**: Simplified analysis for faster processing

### 3. Custom S3 Configuration

Update `workcomp_rates/settings.py` to modify S3 settings:

```python
AWS_S3_BUCKET = 'your-custom-bucket'
AWS_S3_PREFIX = 'your-custom-prefix/'
```

## Troubleshooting

### Common Issues

#### 1. AWS Credentials Error
```
Error: Failed to connect to S3
```
**Solution**: Verify your AWS credentials in the `.env` file or AWS CLI configuration.

#### 2. No Partitions Found
```
Error: No partitions found matching the selected criteria
```
**Solution**: Ensure your partition navigation database is up to date and contains the expected partitions.

#### 3. Memory Issues
```
Error: Out of memory
```
**Solution**: Reduce the `max_rows` parameter or increase server memory.

#### 4. Slow Loading
**Solution**: 
- Check network connectivity to S3
- Consider using S3 Transfer Acceleration
- Reduce `max_rows` parameter
- Check S3 bucket region proximity

### Debug Mode

Enable debug logging by setting:

```python
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
        },
    },
    'root': {
        'level': 'DEBUG',
        'handlers': ['console'],
    },
}
```

## Security Considerations

### 1. Credential Management
- Never commit AWS credentials to version control
- Use IAM roles in production environments
- Rotate access keys regularly

### 2. Data Access
- Implement proper IAM policies
- Use S3 bucket policies for additional security
- Consider VPC endpoints for private access

### 3. Network Security
- Use HTTPS for all communications
- Consider VPN or private network access
- Implement proper firewall rules

## Monitoring and Maintenance

### 1. Performance Monitoring
- Monitor S3 API calls and costs
- Track memory usage patterns
- Log load times and error rates

### 2. Data Quality
- Regular validation of partition data
- Monitor for schema changes
- Implement data quality alerts

### 3. Updates
- Keep boto3 library updated
- Monitor AWS service updates
- Regular testing of S3 connectivity

## Support

For technical support or questions:
1. Check the Django logs in `logs/django.log`
2. Review AWS CloudWatch logs
3. Verify S3 bucket permissions
4. Test with smaller datasets first

## Example Workflow

1. **Setup**: Configure AWS credentials and S3 bucket
2. **Filter**: Select payer, state, and billing class filters
3. **Review**: Click "Review Dataset" to load and analyze data
4. **Explore**: Review data quality, statistics, and business insights across multiple tabs
5. **Export**: Download the combined dataset for further analysis
6. **Iterate**: Adjust filters and repeat as needed

This streamlined system provides a single, powerful tool to explore and analyze your partitioned healthcare data with comprehensive insights and export capabilities.
