import os
import sys
import unittest
import json
import gzip
from unittest.mock import patch, MagicMock, ANY
from datetime import datetime
from io import BytesIO
from botocore.exceptions import ClientError

# Add project root to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.storage.s3_storage import S3Storage


class TestS3Storage(unittest.TestCase):
    """Tests for the S3Storage class."""
    
    def setUp(self):
        """Set up test fixtures."""
        # We'll patch boto3.client in individual tests
        self.sample_data = {
            "test": "data",
            "nested": {
                "key": "value"
            },
            "list": [1, 2, 3]
        }
    
    @patch("boto3.client")
    def test_init(self, mock_boto3_client):
        """Test initialization of S3Storage."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        
        s3_storage = S3Storage()
        
        mock_boto3_client.assert_called_once_with(
            's3',
            aws_access_key_id=s3_storage.aws_access_key_id,
            aws_secret_access_key=s3_storage.aws_secret_access_key
        )
    
    def test_get_date_path(self):
        """Test getting date-based path."""
        s3_storage = S3Storage()
        date_path = s3_storage._get_date_path()
        today = datetime.now()
        
        expected_path = f"{today.year}/{today.month:02d}/{today.day:02d}"
        self.assertEqual(date_path, expected_path)
    
    def test_generate_key(self):
        """Test generating S3 key."""
        s3_storage = S3Storage()
        source = "test_source"
        data_type = "raw"
        filename = "test.json"
        
        date_path = s3_storage._get_date_path()
        expected_key = f"raw/test_source/{date_path}/test.json"
        
        key = s3_storage._generate_key(source, data_type, filename)
        self.assertEqual(key, expected_key)
    
    @patch("boto3.client")
    def test_store_raw_data(self, mock_boto3_client):
        """Test storing raw data."""
        # Setup
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        
        with patch.object(S3Storage, "_generate_key") as mock_generate_key:
            mock_generate_key.return_value = "raw/test/2023/01/01/test.json.gz"
            
            s3_storage = S3Storage()
            source = "test"
            filename = "test.json.gz"
            
            # Execute
            key = s3_storage.store_raw_data(source, self.sample_data, filename)
            
            # Assert
            mock_generate_key.assert_called_once_with(source, "raw", filename)
            mock_client.put_object.assert_called_once_with(
                Bucket=s3_storage.bucket_name,
                Key="raw/test/2023/01/01/test.json.gz",
                Body=ANY,
                ContentType='application/json',
                ContentEncoding='gzip'
            )
            
            # Verify that the body is compressed JSON
            call_args = mock_client.put_object.call_args
            body_arg = call_args[1]["Body"]
            
            # The body should be gzipped JSON
            self.assertIsInstance(body_arg, bytes)
    
    @patch("boto3.client")
    def test_store_processed_data_json(self, mock_boto3_client):
        """Test storing processed data as JSON."""
        # Setup
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        
        with patch.object(S3Storage, "_generate_key") as mock_generate_key:
            mock_generate_key.return_value = "processed/test/2023/01/01/test.json"
            
            s3_storage = S3Storage()
            source = "test"
            filename = "test.json"
            
            # Execute
            key = s3_storage.store_processed_data(source, self.sample_data, 'json', filename)
            
            # Assert
            mock_generate_key.assert_called_once_with(source, "processed", filename)
            mock_client.put_object.assert_called_once_with(
                Bucket=s3_storage.bucket_name,
                Key="processed/test/2023/01/01/test.json",
                Body=ANY,
                ContentType='application/json',
                ContentEncoding='gzip'
            )
    
    @patch("boto3.client")
    def test_check_file_exists_true(self, mock_boto3_client):
        """Test checking if a file exists (file exists)."""
        # Setup
        mock_client = MagicMock()
        mock_client.head_object.return_value = {}
        mock_boto3_client.return_value = mock_client
        
        s3_storage = S3Storage()
        
        # Execute
        result = s3_storage.check_file_exists("test/file.json")
        
        # Assert
        self.assertTrue(result)
        mock_client.head_object.assert_called_once_with(
            Bucket=s3_storage.bucket_name,
            Key="test/file.json"
        )
    
    @patch("boto3.client")
    def test_check_file_exists_false(self, mock_client):
        """Test checking if a file does not exist."""
        # Mock boto3 client
        mock_s3 = MagicMock()
        # Set up the mock to raise ClientError with 404 status
        error_response = {'Error': {'Code': '404'}}
        mock_s3.head_object.side_effect = ClientError(error_response, 'head_object')
        mock_client.return_value = mock_s3
        
        # Check if file exists
        s3_storage = S3Storage()
        file_exists = s3_storage.check_file_exists("nonexistent/key.json")
        
        # Assert
        self.assertFalse(file_exists)
        mock_s3.head_object.assert_called_with(Bucket=s3_storage.bucket_name, Key="nonexistent/key.json")
    
    @patch("boto3.client")
    def test_list_files(self, mock_boto3_client):
        """Test listing files with a prefix."""
        # Setup
        mock_client = MagicMock()
        mock_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "prefix/file1.json"},
                {"Key": "prefix/file2.json"}
            ]
        }
        mock_boto3_client.return_value = mock_client
        
        s3_storage = S3Storage()
        
        # Execute
        result = s3_storage.list_files("prefix")
        
        # Assert
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], "prefix/file1.json")
        self.assertEqual(result[1], "prefix/file2.json")
        
        mock_client.list_objects_v2.assert_called_once_with(
            Bucket=s3_storage.bucket_name,
            Prefix="prefix"
        )
    
    @patch("boto3.client")
    def test_list_files_empty(self, mock_boto3_client):
        """Test listing files when none exist."""
        # Setup
        mock_client = MagicMock()
        mock_client.list_objects_v2.return_value = {}
        mock_boto3_client.return_value = mock_client
        
        s3_storage = S3Storage()
        
        # Execute
        result = s3_storage.list_files("prefix")
        
        # Assert
        self.assertEqual(result, [])
    
    @patch("boto3.client")
    def test_load_data_json(self, mock_boto3_client):
        """Test loading JSON data."""
        # Setup
        mock_client = MagicMock()
        
        # Create mock response
        json_data = json.dumps(self.sample_data).encode('utf-8')
        compressed_data = gzip.compress(json_data)
        
        mock_body = MagicMock()
        mock_body.read.return_value = compressed_data
        
        mock_client.get_object.return_value = {
            "ContentType": "application/json",
            "ContentEncoding": "gzip",
            "Body": mock_body
        }
        mock_boto3_client.return_value = mock_client
        
        s3_storage = S3Storage()
        
        # Execute
        result = s3_storage.load_data("test/file.json")
        
        # Assert
        self.assertEqual(result, self.sample_data)
        mock_client.get_object.assert_called_once_with(
            Bucket=s3_storage.bucket_name,
            Key="test/file.json"
        )
    
    def test_real_s3_connection(self):
        """Test actual S3 connection with real credentials."""
        try:
            # Initialize S3 storage with real credentials
            s3_storage = S3Storage()
            
            # Test bucket access
            s3_storage.s3_client.head_bucket(Bucket=s3_storage.bucket_name)
            
            # Test write access
            test_key = "test_connection.txt"
            test_data = "Test connection successful"
            
            s3_storage.s3_client.put_object(
                Bucket=s3_storage.bucket_name,
                Key=test_key,
                Body=test_data.encode('utf-8')
            )
            
            # Test read access
            response = s3_storage.s3_client.get_object(
                Bucket=s3_storage.bucket_name,
                Key=test_key
            )
            
            # Clean up test file
            s3_storage.s3_client.delete_object(
                Bucket=s3_storage.bucket_name,
                Key=test_key
            )
            
            self.assertTrue(True, "S3 connection test passed")
            
        except Exception as e:
            self.fail(f"S3 connection test failed: {str(e)}")


if __name__ == "__main__":
    unittest.main() 