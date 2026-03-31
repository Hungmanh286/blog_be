import json
import uuid
import logging
from io import BytesIO
from typing import Optional

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError, EndpointConnectionError

from app.core.config import settings

logger = logging.getLogger(__name__)


class S3Service:
    """
    Service for managing file uploads to MinIO (S3-compatible storage).

    - MINIO_ENDPOINT:   Internal endpoint used by the app to connect (e.g. http://minio:9000)
    - MINIO_PUBLIC_URL: Public URL used by clients to load the image (e.g. http://localhost:9000)
    """

    def __init__(self):
        self.client = boto3.client(
            "s3",
            endpoint_url=settings.MINIO_ENDPOINT,      # http://minio:9000 inside docker
            aws_access_key_id=settings.MINIO_ACCESS_KEY,
            aws_secret_access_key=settings.MINIO_SECRET_KEY,
            region_name="us-east-1",
            # Required for MinIO path-style access (not virtual-hosted)
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )
        self.bucket_name = settings.MINIO_BUCKET_NAME
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self):
        """Create the bucket if it doesn't exist and apply a public-read policy."""
        try:
            self.client.head_bucket(Bucket=self.bucket_name)
            logger.info("MinIO bucket '%s' already exists.", self.bucket_name)
        except ClientError as e:
            error_code = int(e.response["Error"]["Code"])
            if error_code == 404:
                logger.info("Creating MinIO bucket '%s'...", self.bucket_name)
                self.client.create_bucket(Bucket=self.bucket_name)
                self._set_public_read_policy()
            else:
                logger.error("Unexpected error checking MinIO bucket: %s", e)
                raise
        except EndpointConnectionError:
            logger.error(
                "Cannot connect to MinIO at %s — make sure the minio service is running.",
                settings.MINIO_ENDPOINT,
            )
            raise

    def _set_public_read_policy(self):
        """Set bucket policy to allow public GET access for all objects."""
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": ["*"]},
                    "Action": ["s3:GetObject"],
                    "Resource": [f"arn:aws:s3:::{self.bucket_name}/*"],
                }
            ],
        }
        self.client.put_bucket_policy(
            Bucket=self.bucket_name,
            Policy=json.dumps(policy),
        )
        logger.info("Public-read policy applied to bucket '%s'.", self.bucket_name)

    def upload_file(
        self,
        file_content: bytes,
        original_filename: str,
        content_type: str = "image/jpeg",
        folder: str = "hero-images",
    ) -> str:
        """
        Upload a file to MinIO and return the public URL.

        Args:
            file_content:      Raw bytes of the file.
            original_filename: Original filename (used to preserve extension).
            content_type:      MIME type (e.g. "image/jpeg").
            folder:            Sub-folder prefix inside the bucket.

        Returns:
            Public URL that clients can use to load the image.
            Format: {MINIO_PUBLIC_URL}/{bucket}/{folder}/{uuid}.{ext}
        """
        ext = original_filename.rsplit(".", 1)[-1].lower() if "." in original_filename else "jpg"
        unique_filename = f"{uuid.uuid4().hex}.{ext}"
        object_key = f"{folder}/{unique_filename}"

        self.client.put_object(
            Bucket=self.bucket_name,
            Key=object_key,
            Body=BytesIO(file_content),
            ContentLength=len(file_content),
            ContentType=content_type,
        )

        # Build the public URL using MINIO_PUBLIC_URL (accessible by browser/clients)
        public_url = f"{settings.MINIO_PUBLIC_URL.rstrip('/')}/{self.bucket_name}/{object_key}"
        logger.info("Uploaded '%s' → %s", original_filename, public_url)
        return public_url

    def delete_file(self, file_url: str) -> bool:
        """
        Delete a file from MinIO by its public URL.

        Extracts the object key from the URL and calls delete_object.
        Silently returns False if the URL doesn't belong to this bucket.

        Args:
            file_url: Full public URL stored in the DB (e.g. http://localhost:9000/blog-images/hero-images/abc.jpg)

        Returns:
            True if deleted, False if URL is unrecognised or deletion failed.
        """
        try:
            # Strip the public base URL to get the bucket/key portion
            base = f"{settings.MINIO_PUBLIC_URL.rstrip('/')}/{self.bucket_name}/"
            if not file_url.startswith(base):
                logger.warning("delete_file: URL '%s' does not match bucket prefix, skipping.", file_url)
                return False

            object_key = file_url[len(base):]
            self.client.delete_object(Bucket=self.bucket_name, Key=object_key)
            logger.info("Deleted object '%s' from MinIO.", object_key)
            return True
        except ClientError as e:
            logger.error("Failed to delete '%s' from MinIO: %s", file_url, e)
            return False

    def get_file_url(self, object_key: str) -> str:
        """Build a public URL for a given object key."""
        return f"{settings.MINIO_PUBLIC_URL.rstrip('/')}/{self.bucket_name}/{object_key}"


# ---------------------------------------------------------------------------
# Lazy singleton — instantiated on first use, not at import time.
# This prevents startup failures when MinIO hasn't finished booting yet.
# ---------------------------------------------------------------------------
_s3_service: Optional[S3Service] = None


def get_s3_service() -> S3Service:
    """Return the shared S3Service instance (created on first call)."""
    global _s3_service
    if _s3_service is None:
        _s3_service = S3Service()
    return _s3_service
