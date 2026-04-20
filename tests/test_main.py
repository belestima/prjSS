import json
import pytest
from unittest.mock import MagicMock, patch, call
import sys
import os

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from main import parse_single_line, lambda_handler


class TestParseSingleLine:
    """Test cases for the parse_single_line function."""

    def test_parse_json_object(self):
        """Test parsing a valid JSON object."""
        json_input = '{"name": "John", "age": 30, "city": "New York"}'
        result = parse_single_line(json_input)
        expected = {"name": "John", "age": 30, "city": "New York"}
        assert result == expected

    def test_parse_json_array(self):
        """Test parsing a valid JSON array."""
        json_input = '[1, 2, 3, "test"]'
        result = parse_single_line(json_input)
        expected = [1, 2, 3, "test"]
        assert result == expected

    def test_parse_json_string(self):
        """Test parsing a JSON string value."""
        json_input = '"Hello, World!"'
        result = parse_single_line(json_input)
        expected = "Hello, World!"
        assert result == expected

    def test_parse_plain_text(self):
        """Test parsing plain text when JSON parsing fails."""
        text_input = "This is plain text"
        result = parse_single_line(text_input)
        expected = {"content": "This is plain text"}
        assert result == expected

    def test_parse_invalid_json(self):
        """Test parsing invalid JSON falls back to plain text."""
        invalid_json = '{"name": "John", "age": }'
        result = parse_single_line(invalid_json)
        expected = {"content": invalid_json}
        assert result == expected

class TestLambdaHandler:
    """Test cases for the lambda_handler function."""

    @patch('main.s3_client')
    @patch('main.logger')
    def test_lambda_handler_success_single_record(self, mock_logger, mock_s3_client):
        """Test successful lambda handler execution with a single S3 record."""
        # Mock S3 get_object response
        mock_response = {
            'Body': MagicMock()
        }
        mock_response['Body'].read.return_value = b'{"message": "test data"}'
        mock_s3_client.get_object.return_value = mock_response

        # Mock event with single record
        event = {
            'Records': [{
                's3': {
                    'bucket': {'name': 'test-bucket'},
                    'object': {'key': 'input/test-file.json'}
                }
            }]
        }

        # Mock context
        context = MagicMock()

        # Call lambda handler
        result = lambda_handler(event, context)

        # Assertions
        assert result['statusCode'] == 200
        assert 'File processed successfully' in result['body']

        # Verify S3 client was called correctly
        mock_s3_client.get_object.assert_called_once_with(
            Bucket='test-bucket',
            Key='input/test-file.json'
        )

        # Verify logging calls
        assert mock_logger.info.call_count == 2
        mock_logger.info.assert_has_calls([
            call('Processing file: s3://test-bucket/input/test-file.json'),
            call('Parsed data: {\'message\': \'test data\'}')
        ])

    @patch('main.s3_client')
    @patch('main.logger')
    def test_lambda_handler_s3_error(self, mock_logger, mock_s3_client):
        """Test lambda handler with S3 access error."""
        # Mock S3 to raise an exception
        mock_s3_client.get_object.side_effect = Exception("Access denied")

        event = {
            'Records': [{
                's3': {
                    'bucket': {'name': 'test-bucket'},
                    'object': {'key': 'input/test-file.json'}
                }
            }]
        }
        context = MagicMock()

        # Should raise exception
        with pytest.raises(Exception, match="Access denied"):
            lambda_handler(event, context)

        # Verify error logging
        mock_logger.error.assert_called_once_with("Error processing S3 event: Access denied")

    @patch('main.s3_client')
    @patch('main.logger')
    def test_lambda_handler_empty_event(self, mock_logger, mock_s3_client):
        """Test lambda handler with empty records."""
        event = {'Records': []}
        context = MagicMock()

        result = lambda_handler(event, context)

        # Should still return success for empty event
        assert result['statusCode'] == 200
        assert mock_s3_client.get_object.call_count == 0
        assert mock_logger.info.call_count == 0

    @patch('main.s3_client')
    @patch('main.logger')
    def test_lambda_handler_malformed_event(self, mock_logger, mock_s3_client):
        """Test lambda handler with malformed event data."""
        # Event missing required fields
        event = {
            'Records': [{
                's3': {
                    'bucket': {},  # Missing name
                    'object': {}   # Missing key
                }
            }]
        }
        context = MagicMock()

        # Should raise KeyError for missing fields
        with pytest.raises(KeyError):
            lambda_handler(event, context)
