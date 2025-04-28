import os
import json
import gzip
import datetime
from typing import Dict, Any, Optional, Union, List
import boto3
from botocore.exceptions import ClientError
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import gc
import logging

import src.utils.config as config


class S3Storage:
    """Class to handle storage of crawler data in AWS S3."""
    
    def __init__(self):
        """Initialize S3 storage with AWS credentials from config."""
        self.logger = logging.getLogger("s3_storage")
        
        # Verify AWS credentials
        self.aws_access_key_id = config.AWS_ACCESS_KEY_ID
        self.aws_secret_access_key = config.AWS_SECRET_ACCESS_KEY
        self.bucket_name = config.S3_BUCKET_NAME
        self.region = config.AWS_REGION
        
        if not all([self.aws_access_key_id, self.aws_secret_access_key, self.bucket_name]):
            raise ValueError("Missing AWS credentials or bucket name in config")
        
        # Create S3 client
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.region
            )
            
            # Verify bucket exists and is accessible
            self._verify_bucket()
            
        except Exception as e:
            self.logger.error(f"Failed to initialize S3 client: {str(e)}")
            raise
    
    def _verify_bucket(self):
        """Verify that the S3 bucket exists and is accessible."""
        try:
            # First try to list buckets to verify credentials
            self.s3_client.list_buckets()
            
            # Then try to access the specific bucket
            try:
                self.s3_client.head_bucket(Bucket=self.bucket_name)
                self.logger.info(f"Successfully connected to S3 bucket: {self.bucket_name}")
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == '404':
                    self.logger.info(f"Bucket {self.bucket_name} does not exist, creating it...")
                    self._create_bucket()
                elif error_code == '403':
                    self.logger.error(f"Access denied to bucket {self.bucket_name}. Please check your AWS credentials and permissions.")
                    raise ValueError(f"Access denied to bucket {self.bucket_name}. Please check your AWS credentials and permissions.")
                else:
                    self.logger.error(f"Error accessing bucket {self.bucket_name}: {str(e)}")
                    raise
                    
        except ClientError as e:
            if e.response['Error']['Code'] == 'InvalidAccessKeyId':
                self.logger.error("Invalid AWS Access Key ID")
                raise ValueError("Invalid AWS Access Key ID")
            elif e.response['Error']['Code'] == 'SignatureDoesNotMatch':
                self.logger.error("Invalid AWS Secret Access Key")
                raise ValueError("Invalid AWS Secret Access Key")
            else:
                self.logger.error(f"Error verifying AWS credentials: {str(e)}")
                raise
    
    def _create_bucket(self):
        """Create the S3 bucket if it doesn't exist."""
        try:
            # Create bucket
            self.s3_client.create_bucket(
                Bucket=self.bucket_name,
                CreateBucketConfiguration={'LocationConstraint': self.region}
            )
            
            # Wait for bucket to exist
            waiter = self.s3_client.get_waiter('bucket_exists')
            waiter.wait(Bucket=self.bucket_name)
            
            self.logger.info(f"Successfully created bucket: {self.bucket_name}")
            
            # Set bucket policy for public access
            bucket_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "PublicReadGetObject",
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": "s3:GetObject",
                        "Resource": f"arn:aws:s3:::{self.bucket_name}/*"
                    }
                ]
            }
            
            # Convert policy to JSON string
            policy_string = json.dumps(bucket_policy)
            
            # Set bucket policy
            self.s3_client.put_bucket_policy(
                Bucket=self.bucket_name,
                Policy=policy_string
            )
            
            self.logger.info(f"Successfully set bucket policy for: {self.bucket_name}")
            
        except ClientError as e:
            self.logger.error(f"Failed to create bucket {self.bucket_name}: {str(e)}")
            raise
    
    def _get_date_path(self) -> str:
        """
        Get date-based path for organizing data.
        
        Returns:
            str: Date path in YYYY/MM/DD format
        """
        today = datetime.datetime.now()
        return f"{today.year}/{today.month:02d}/{today.day:02d}"
    
    def _generate_key(self, source: str, data_type: str, filename: str) -> str:
        """
        Generate S3 key with date-based partitioning.
        
        Args:
            source: Data source (e.g., website name)
            data_type: Type of data (raw, processed, or text_processed)
            filename: Filename
            
        Returns:
            str: Generated S3 key
        """
        date_path = self._get_date_path()
        return f"{data_type}/{source}/{date_path}/{filename}"
    
    def store_raw_data(self, source: str, data: Dict[str, Any], filename: Optional[str] = None) -> str:
        """
        Store raw data in S3.
        
        Args:
            source: Data source (e.g., website name)
            data: Raw data to store
            filename: Optional filename (default: source_timestamp.json.gz)
            
        Returns:
            str: S3 key where data was stored
        """
        if not filename:
            timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            filename = f"{source}_{timestamp}.json.gz"
        
        # Generate S3 key
        s3_key = self._generate_key(source, config.S3_RAW_DATA_PREFIX, filename)
        
        # Upload to S3
        try:
            # Compress JSON data
            json_data = json.dumps(data).encode('utf-8')
            compressed_data = gzip.compress(json_data)
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=compressed_data,
                ContentType='application/json',
                ContentEncoding='gzip'
            )
            print(f"Stored raw data at s3://{self.bucket_name}/{s3_key}")
            return s3_key
        except ClientError as e:
            print(f"Error storing raw data: {str(e)}")
            raise

    def stream_raw_data(self, source: str, data_generator, filename: Optional[str] = None) -> str:
        """
        Stream raw data to S3 as it's collected.
        
        Args:
            source: Data source (e.g., website name)
            data_generator: Generator yielding data chunks
            filename: Optional filename (default: source_timestamp.json.gz)
            
        Returns:
            str: S3 key where data was stored
        """
        if not filename:
            timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            filename = f"{source}_{timestamp}.json.gz"
        
        # Generate S3 key
        s3_key = self._generate_key(source, config.S3_RAW_DATA_PREFIX, filename)
        
        try:
            # Collect all data first to determine size
            all_data = []
            for chunk in data_generator:
                all_data.append(chunk)
            
            # If data is small, use regular put_object
            if len(all_data) == 0:
                self.logger.warning("No data to upload")
                return s3_key
                
            # Compress all data
            json_data = json.dumps(all_data).encode('utf-8')
            compressed_data = gzip.compress(json_data)
            
            # If data is small (less than 5MB), use regular upload
            if len(compressed_data) < 5 * 1024 * 1024:  # 5MB
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    Body=compressed_data,
                    ContentType='application/json',
                    ContentEncoding='gzip'
                )
                print(f"Stored raw data at s3://{self.bucket_name}/{s3_key}")
                return s3_key
            
            # For larger data, use multipart upload
            mpu = self.s3_client.create_multipart_upload(
                Bucket=self.bucket_name,
                Key=s3_key,
                ContentType='application/json',
                ContentEncoding='gzip'
            )
            
            parts = []
            part_number = 1
            chunk_size = 5 * 1024 * 1024  # 5MB chunks
            
            # Split data into chunks
            for i in range(0, len(compressed_data), chunk_size):
                chunk = compressed_data[i:i + chunk_size]
                
                # Upload part
                part = self.s3_client.upload_part(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    PartNumber=part_number,
                    UploadId=mpu['UploadId'],
                    Body=chunk
                )
                
                parts.append({
                    'PartNumber': part_number,
                    'ETag': part['ETag']
                })
                part_number += 1
            
            # Complete multipart upload
            self.s3_client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=s3_key,
                UploadId=mpu['UploadId'],
                MultipartUpload={'Parts': parts}
            )
            
            print(f"Streamed raw data to s3://{self.bucket_name}/{s3_key}")
            return s3_key
            
        except ClientError as e:
            # Abort multipart upload on error
            if 'UploadId' in locals():
                try:
                    self.s3_client.abort_multipart_upload(
                        Bucket=self.bucket_name,
                        Key=s3_key,
                        UploadId=mpu['UploadId']
                    )
                except Exception as abort_error:
                    self.logger.error(f"Failed to abort multipart upload: {str(abort_error)}")
            print(f"Error streaming raw data: {str(e)}")
            raise
    
    def store_processed_data(self, source: str, data: Dict[str, Any], 
                           format: str = 'json', filename: Optional[str] = None) -> str:
        """
        Store processed data in S3.
        
        Args:
            source: Data source (e.g., website name)
            data: Processed data to store
            format: Storage format ('json', 'parquet', or 'csv')
            filename: Optional filename (default: source_timestamp.<format>)
            
        Returns:
            str: S3 key where data was stored
        """
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        
        if not filename:
            filename = f"{source}_{timestamp}.{format}"
        
        # Generate S3 key
        s3_key = self._generate_key(source, config.S3_PROCESSED_DATA_PREFIX, filename)
        
        try:
            if format == 'json':
                # Compress JSON data
                json_data = json.dumps(data).encode('utf-8')
                compressed_data = gzip.compress(json_data)
                
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    Body=compressed_data,
                    ContentType='application/json',
                    ContentEncoding='gzip'
                )
            
            elif format == 'parquet':
                # Convert to pandas DataFrame
                df = pd.DataFrame(data)
                
                # Convert to PyArrow Table and write to buffer
                table = pa.Table.from_pandas(df)
                
                # Write to buffer
                buffer = pa.BufferOutputStream()
                pq.write_table(table, buffer)
                
                # Upload to S3
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    Body=buffer.getvalue().to_pybytes(),
                    ContentType='application/octet-stream'
                )
            
            elif format == 'csv':
                # Convert to pandas DataFrame and CSV
                df = pd.DataFrame(data)
                csv_data = df.to_csv(index=False).encode('utf-8')
                compressed_data = gzip.compress(csv_data)
                
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    Body=compressed_data,
                    ContentType='text/csv',
                    ContentEncoding='gzip'
                )
            
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            print(f"Stored processed data at s3://{self.bucket_name}/{s3_key}")
            return s3_key
        
        except ClientError as e:
            print(f"Error storing processed data: {str(e)}")
            raise
    
    def store_processed_text_data(self, source: str, data: Dict[str, Any], filename: Optional[str] = None) -> str:
        """
        Store processed text data (NER, keywords, etc.) in S3.
        
        Args:
            source: Data source (e.g., website name)
            data: Processed text data to store
            filename: Optional filename (default: source_timestamp_text.json.gz)
            
        Returns:
            str: S3 key where data was stored
        """
        if not filename:
            timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            filename = f"{source}_{timestamp}_text.json.gz"
        
        # Generate S3 key
        s3_key = self._generate_key(source, "text_processed", filename)
        
        try:
            # Compress JSON data
            json_data = json.dumps(data).encode('utf-8')
            compressed_data = gzip.compress(json_data)
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=compressed_data,
                ContentType='application/json',
                ContentEncoding='gzip'
            )
            print(f"Stored processed text data at s3://{self.bucket_name}/{s3_key}")
            return s3_key
        except ClientError as e:
            print(f"Error storing processed text data: {str(e)}")
            raise

    def stream_processed_text_data(self, source: str, data_generator, filename: Optional[str] = None) -> str:
        """
        Stream processed text data to S3 as it's collected.
        
        Args:
            source: Data source (e.g., website name)
            data_generator: Generator yielding processed text data chunks
            filename: Optional filename (default: source_timestamp_text.json.gz)
            
        Returns:
            str: S3 key where data was stored
        """
        if not filename:
            timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            filename = f"{source}_{timestamp}_text.json.gz"
        
        # Generate S3 key
        s3_key = self._generate_key(source, "text_processed", filename)
        
        try:
            self.logger.info(f"Starting multipart upload to s3://{self.bucket_name}/{s3_key}")
            
            # Initialize multipart upload
            mpu = self.s3_client.create_multipart_upload(
                Bucket=self.bucket_name,
                Key=s3_key,
                ContentType='application/json',
                ContentEncoding='gzip'
            )
            
            parts = []
            part_number = 1
            buffer = []
            buffer_size = 0
            max_buffer_size = 5 * 1024 * 1024  # 5MB buffer
            
            # Process each chunk
            for chunk in data_generator:
                # Add chunk to buffer
                buffer.append(chunk)
                buffer_size += len(str(chunk))
                
                # If buffer is full, upload it
                if buffer_size >= max_buffer_size:
                    self.logger.debug(f"Uploading part {part_number} (size: {buffer_size} bytes)")
                    
                    # Compress buffer
                    json_data = json.dumps(buffer).encode('utf-8')
                    compressed_data = gzip.compress(json_data)
                    
                    # Upload part
                    part = self.s3_client.upload_part(
                        Bucket=self.bucket_name,
                        Key=s3_key,
                        PartNumber=part_number,
                        UploadId=mpu['UploadId'],
                        Body=compressed_data
                    )
                    
                    parts.append({
                        'PartNumber': part_number,
                        'ETag': part['ETag']
                    })
                    part_number += 1
                    
                    # Clear buffer
                    buffer = []
                    buffer_size = 0
                    
                    # Force garbage collection
                    gc.collect()
            
            # Upload remaining buffer if any
            if buffer:
                self.logger.debug(f"Uploading final part {part_number} (size: {buffer_size} bytes)")
                json_data = json.dumps(buffer).encode('utf-8')
                compressed_data = gzip.compress(json_data)
                
                part = self.s3_client.upload_part(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    PartNumber=part_number,
                    UploadId=mpu['UploadId'],
                    Body=compressed_data
                )
                
                parts.append({
                    'PartNumber': part_number,
                    'ETag': part['ETag']
                })
            
            # Complete multipart upload
            self.s3_client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=s3_key,
                UploadId=mpu['UploadId'],
                MultipartUpload={'Parts': parts}
            )
            
            self.logger.info(f"Successfully uploaded data to s3://{self.bucket_name}/{s3_key}")
            return s3_key
            
        except ClientError as e:
            # Abort multipart upload on error
            if 'UploadId' in locals():
                try:
                    self.s3_client.abort_multipart_upload(
                        Bucket=self.bucket_name,
                        Key=s3_key,
                        UploadId=mpu['UploadId']
                    )
                except Exception as abort_error:
                    self.logger.error(f"Failed to abort multipart upload: {str(abort_error)}")
            
            self.logger.error(f"Error streaming data to S3: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error during S3 upload: {str(e)}")
            raise
    
    def check_file_exists(self, key: str) -> bool:
        """
        Check if a file exists in S3.
        
        Args:
            key: S3 key
            
        Returns:
            bool: True if file exists, False otherwise
        """
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                raise
    
    def list_files(self, prefix: str) -> List[str]:
        """
        List files in S3 with given prefix.
        
        Args:
            prefix: S3 key prefix
            
        Returns:
            List[str]: List of S3 keys
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            if 'Contents' in response:
                return [obj['Key'] for obj in response['Contents']]
            else:
                return []
        
        except ClientError as e:
            print(f"Error listing files: {str(e)}")
            raise
    
    def load_data(self, key: str) -> Dict[str, Any]:
        """
        Load data from S3.
        
        Args:
            key: S3 key
            
        Returns:
            Dict[str, Any]: Loaded data
        """
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=key
            )
            
            content_type = response.get('ContentType', '')
            content_encoding = response.get('ContentEncoding', '')
            body = response['Body'].read()
            
            if content_encoding == 'gzip':
                body = gzip.decompress(body)
            
            if 'json' in content_type:
                return json.loads(body.decode('utf-8'))
            elif 'parquet' in content_type or key.endswith('.parquet'):
                # Read parquet from buffer
                table = pq.read_table(pa.py_buffer(body))
                return table.to_pandas().to_dict(orient='records')
            elif 'csv' in content_type or key.endswith('.csv'):
                return pd.read_csv(body).to_dict(orient='records')
            else:
                return {"raw": body.decode('utf-8')}
        
        except ClientError as e:
            print(f"Error loading data: {str(e)}")
            raise 