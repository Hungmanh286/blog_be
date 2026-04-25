import json
import uuid
import logging
from io import BytesIO
from typing import Optional
from urllib.parse import urlparse

from minio import Minio
from minio.error import S3Error

from app.core.config import settings

logger = logging.getLogger(__name__)


class S3Service:
    """
    Dịch vụ quản lý tệp trên MinIO sử dụng bộ SDK chính thức của MinIO.

    Tối ưu hóa:
    1. Nhẹ hơn boto3.
    2. API đơn giản, chuyên cho MinIO.
    3. Hỗ trợ tốt cho FastAPI.
    """

    def __init__(self):
        parsed_url = urlparse(settings.MINIO_ENDPOINT)
        endpoint_host = parsed_url.netloc if parsed_url.netloc else parsed_url.path
        secure = parsed_url.scheme == "https"

        self.client = Minio(
            endpoint_host,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=secure,
        )
        self.bucket_name = settings.MINIO_BUCKET_NAME
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self):
        """Kiểm tra và tạo bucket nếu chưa có, đồng thời thiết lập quyền đọc công khai."""
        try:
            if not self.client.bucket_exists(self.bucket_name):
                logger.info("Đang tạo bucket MinIO: '%s'...", self.bucket_name)
                self.client.make_bucket(self.bucket_name)
                self._set_public_read_policy()
            else:
                logger.info("Bucket '%s' đã tồn tại.", self.bucket_name)
        except Exception as e:
            logger.error("Lỗi khi kết nối với MinIO: %s", e)
            # Không raise ở init để app vẫn có thể khởi động nếu MinIO chưa kịp online
            pass

    def _set_public_read_policy(self):
        """Thiết lập quyền cho phép mọi người xem ảnh qua link trực tiếp."""
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
        self.client.set_bucket_policy(self.bucket_name, json.dumps(policy))
        logger.info(
            "Đã thiết lập quyền đọc công khai cho bucket '%s'.", self.bucket_name
        )

    def upload_file(
        self,
        file_content: bytes,
        original_filename: str,
        content_type: str = "image/jpeg",
        folder: str = "hero-images",
    ) -> str:
        """
        Tải file lên MinIO và trả về link truy cập công khai.
        """
        try:
            ext = (
                original_filename.rsplit(".", 1)[-1].lower()
                if "." in original_filename
                else "jpg"
            )
            unique_filename = f"{uuid.uuid4().hex}.{ext}"
            object_key = f"{folder}/{unique_filename}"

            # Sử dụng put_object cho dữ liệu trong bộ nhớ
            self.client.put_object(
                self.bucket_name,
                object_key,
                BytesIO(file_content),
                length=len(file_content),
                content_type=content_type,
            )

            # Trả về URL dùng để hiển thị trên web
            public_url = f"{settings.MINIO_PUBLIC_URL.rstrip('/')}/{self.bucket_name}/{object_key}"
            logger.info("Đã tải lên '%s' → %s", original_filename, public_url)
            return public_url
        except Exception as e:
            logger.error("Lỗi khi tải file lên MinIO: %s", e)
            raise

    def delete_file(self, file_url: str) -> bool:
        """
        Xóa file khỏi MinIO dựa trên URL.
        """
        try:
            # Lấy object_key từ URL
            base = f"{settings.MINIO_PUBLIC_URL.rstrip('/')}/{self.bucket_name}/"
            if not file_url.startswith(base):
                return False

            object_key = file_url[len(base) :]
            self.client.remove_object(self.bucket_name, object_key)
            logger.info("Đã xóa file '%s' khỏi MinIO.", object_key)
            return True
        except Exception as e:
            logger.error("Lỗi khi xóa file khỏi MinIO: %s", e)
            return False


# Lazy singleton pattern
_s3_service: Optional[S3Service] = None


def get_s3_service() -> S3Service:
    global _s3_service
    if _s3_service is None:
        _s3_service = S3Service()
    return _s3_service
