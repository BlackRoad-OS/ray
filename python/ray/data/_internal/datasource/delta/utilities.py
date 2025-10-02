"""
Delta Lake utility functions for credential management and table operations.
"""

from typing import Dict, Optional

from deltalake import DeltaTable


class AWSUtilities:
    """AWS credential management."""

    @staticmethod
    def get_s3_storage_options() -> Dict[str, str]:
        """Get S3 storage options with automatic credential detection."""
        try:
            import boto3

            session = boto3.Session()
            credentials = session.get_credentials()

            if credentials:
                return {
                    "AWS_ACCESS_KEY_ID": credentials.access_key,
                    "AWS_SECRET_ACCESS_KEY": credentials.secret_key,
                    "AWS_SESSION_TOKEN": credentials.token,
                    "AWS_REGION": session.region_name or "us-east-1",
                }
        except Exception:
            pass
        return {}


class GCPUtilities:
    """GCP credential management."""

    pass


class AzureUtilities:
    """Azure credential management."""

    @staticmethod
    def get_azure_storage_options() -> Dict[str, str]:
        """Get Azure storage options with automatic credential detection."""
        try:
            from azure.identity import DefaultAzureCredential

            credential = DefaultAzureCredential()
            token = credential.get_token("https://storage.azure.com/.default")
            return {"AZURE_STORAGE_TOKEN": token.token}
        except Exception:
            pass
        return {}


def try_get_deltatable(
    table_uri: str, storage_options: Optional[Dict[str, str]] = None
) -> Optional[DeltaTable]:
    """
    Try to get a DeltaTable object, returning None if it doesn't exist.

    Args:
        table_uri: Path to the Delta table
        storage_options: Storage options for cloud authentication

    Returns:
        DeltaTable object if successful, None otherwise
    """
    try:
        return DeltaTable(table_uri, storage_options=storage_options)
    except Exception:
        return None


class DeltaUtilities:
    """Utility class for Delta Lake operations."""

    def __init__(self, path: str, storage_options: Optional[Dict[str, str]] = None):
        """
        Initialize Delta utilities.

        Args:
            path: Path to the Delta table
            storage_options: Storage options for cloud authentication
        """
        self.path = path
        self.storage_options = self._get_storage_options(path, storage_options or {})

    def _get_storage_options(
        self, path: str, provided: Dict[str, str]
    ) -> Dict[str, str]:
        """Get storage options with auto-detection and user overrides."""
        auto_options = {}

        # Auto-detect based on path scheme
        if path.lower().startswith(("s3://", "s3a://")):
            auto_options = AWSUtilities.get_s3_storage_options()
        elif path.lower().startswith(("abfss://", "abfs://")):
            auto_options = AzureUtilities.get_azure_storage_options()

        # Merge with provided (provided takes precedence)
        return {**auto_options, **provided}

    def get_table(self) -> Optional[DeltaTable]:
        """Get the DeltaTable object."""
        return try_get_deltatable(self.path, self.storage_options)
