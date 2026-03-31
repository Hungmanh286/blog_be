from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Response, status, Query, UploadFile, File, Form

from app.db.base import Session, get_db
from app.schemas.sche_post import PostCreate, PostUpdate, PostResponse
from app.services.srv_post import PostService
from app.services.srv_s3 import get_s3_service

import json


def get_post_service(session: Session = Depends(get_db)) -> PostService:
    return PostService(session)


router = APIRouter(tags=["posts"])


@router.post("/", response_model=PostResponse, status_code=status.HTTP_201_CREATED)
async def create_post(
    category: str = Form(...),
    description: str = Form(...),
    title: str = Form(...),
    date: str = Form(...),
    content: str = Form(..., description="TipTap JSON content as string"),
    area: str = Form(...),
    url: str = Form(...),
    hero_image: Optional[UploadFile] = File(None, description="Hero image file to upload"),
    hero_image_url: Optional[str] = Form(None, description="Hero image URL (used if no file uploaded)"),
    service: PostService = Depends(get_post_service),
):
    """Create a new post with optional hero image upload to S3/MinIO.

    You can either:
    - Upload a file via `hero_image` (will be stored in MinIO, URL auto-generated)
    - Provide a URL via `hero_image_url` (stored directly)
    - If neither is provided, heroImage will be empty string
    """
    # Handle hero image
    hero_image_link = ""
    if hero_image and hero_image.filename:
        # Validate file type
        allowed_types = {"image/jpeg", "image/png", "image/webp", "image/gif", "image/svg+xml"}
        if hero_image.content_type not in allowed_types:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid file type '{hero_image.content_type}'. Allowed: {', '.join(allowed_types)}"
            )

        # Read and upload
        file_content = await hero_image.read()
        hero_image_link = get_s3_service().upload_file(
            file_content=file_content,
            original_filename=hero_image.filename,
            content_type=hero_image.content_type,
        )
    elif hero_image_url:
        hero_image_link = hero_image_url

    # Parse content JSON string
    try:
        content_dict = json.loads(content)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON in 'content' field")

    post_data = PostCreate(
        category=category,
        description=description,
        title=title,
        date=date,
        heroImage=hero_image_link,
        content=content_dict,
        area=area,
        url=url,
    )

    post = service.create_post(post_data)
    return post


@router.get("/areas", response_model=List[str])
def read_areas(service: PostService = Depends(get_post_service)):
    """Get list of all distinct areas"""
    return service.get_areas()


@router.get("/categories", response_model=List[str])
def read_categories(service: PostService = Depends(get_post_service)):
    """Get list of all distinct categories"""
    return service.get_categories()


@router.get("/archives")
def read_archives(service: PostService = Depends(get_post_service)):
    """Get list of stored months and years"""
    return service.get_archives()


@router.get("/{post_id}", response_model=PostResponse)
def read_post(post_id: int, service: PostService = Depends(get_post_service)):
    """Get a post by ID"""
    post = service.get_post(post_id)
    if not post:
        raise HTTPException(status_code=404, detail="Post not found")
    return post


@router.get("/", response_model=List[PostResponse])
def read_posts(
    area: Optional[str] = Query(None, description="Lọc theo khu vực (area)"),
    category: Optional[str] = Query(None, description="Lọc theo chuỗi bài (category)"),
    month: Optional[int] = Query(None, ge=1, le=12, description="Lọc theo tháng"),
    year: Optional[int] = Query(None, description="Lọc theo năm"),
    q: Optional[str] = Query(
        None, description="Tìm kiếm theo tiêu đề, mô tả và nội dung"
    ),
    service: PostService = Depends(get_post_service),
):
    """Get all posts (with optional filters)"""
    return service.get_posts(area=area, category=category, month=month, year=year, q=q)


@router.put("/{post_id}", response_model=PostResponse)
async def update_post(
    post_id: int,
    category: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    title: Optional[str] = Form(None),
    date: Optional[str] = Form(None),
    content: Optional[str] = Form(None, description="TipTap JSON content as string"),
    area: Optional[str] = Form(None),
    url: Optional[str] = Form(None),
    hero_image: Optional[UploadFile] = File(None, description="New hero image file"),
    hero_image_url: Optional[str] = Form(None, description="New hero image URL"),
    service: PostService = Depends(get_post_service),
):
    """Update a post with optional hero image re-upload.

    If a new hero_image file is uploaded, the old one will be deleted from S3
    and replaced with the new one.
    """
    # Build update dict from non-None form fields
    update_data = {}
    if category is not None:
        update_data["category"] = category
    if description is not None:
        update_data["description"] = description
    if title is not None:
        update_data["title"] = title
    if date is not None:
        update_data["date"] = date
    if area is not None:
        update_data["area"] = area
    if url is not None:
        update_data["url"] = url

    if content is not None:
        try:
            update_data["content"] = json.loads(content)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid JSON in 'content' field")

    # Handle hero image upload/update
    if hero_image and hero_image.filename:
        allowed_types = {"image/jpeg", "image/png", "image/webp", "image/gif", "image/svg+xml"}
        if hero_image.content_type not in allowed_types:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid file type '{hero_image.content_type}'. Allowed: {', '.join(allowed_types)}"
            )

        # Delete old image from S3 if it exists
        existing_post = service.get_post(post_id)
        if existing_post and existing_post.heroImage:
            get_s3_service().delete_file(existing_post.heroImage)

        file_content = await hero_image.read()
        update_data["heroImage"] = get_s3_service().upload_file(
            file_content=file_content,
            original_filename=hero_image.filename,
            content_type=hero_image.content_type,
        )
    elif hero_image_url is not None:
        # Delete old image from S3 if replacing with a URL
        existing_post = service.get_post(post_id)
        if existing_post and existing_post.heroImage:
            get_s3_service().delete_file(existing_post.heroImage)
        update_data["heroImage"] = hero_image_url

    if not update_data:
        raise HTTPException(status_code=400, detail="No fields to update")

    post = service.update_post(post_id, update_data)
    if not post:
        raise HTTPException(status_code=404, detail="Post not found")
    return post


@router.delete("/{post_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_post(post_id: int, service: PostService = Depends(get_post_service)):
    """Delete a post and its hero image from S3"""
    # Get the post first to delete the hero image
    post = service.get_post(post_id)
    if not post:
        raise HTTPException(status_code=404, detail="Post not found")

    # Delete hero image from S3 if it exists
    if post.heroImage:
        get_s3_service().delete_file(post.heroImage)

    success = service.delete_post(post_id)
    if not success:
        raise HTTPException(status_code=404, detail="Post not found")
    return Response(status_code=status.HTTP_204_NO_CONTENT)
