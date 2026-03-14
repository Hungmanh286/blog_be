from typing import List

from fastapi import APIRouter, Depends, HTTPException, Response, status

from app.db.base import Session, get_db
from app.schemas.sche_post import PostCreate, PostUpdate, PostResponse
from app.services.srv_post import PostService


def get_post_service(session: Session = Depends(get_db)) -> PostService:
    return PostService(session)


router = APIRouter(tags=["posts"])


@router.post("/", response_model=PostResponse, status_code=status.HTTP_201_CREATED)
def create_post(payload: PostCreate, service: PostService = Depends(get_post_service)):
    """Create a new post with TipTap JSON content"""
    post = service.create_post(payload)
    return post


@router.get("/{post_id}", response_model=PostResponse)
def read_post(post_id: int, service: PostService = Depends(get_post_service)):
    """Get a post by ID"""
    post = service.get_post(post_id)
    if not post:
        raise HTTPException(status_code=404, detail="Post not found")
    return post


@router.get("/", response_model=List[PostResponse])
def read_posts(service: PostService = Depends(get_post_service)):
    """Get all posts"""
    return service.get_posts()


@router.put("/{post_id}", response_model=PostResponse)
def update_post(
    post_id: int, payload: PostUpdate, service: PostService = Depends(get_post_service)
):
    """Update a post (including TipTap JSON content)"""
    update_data = payload.dict(exclude_unset=True)
    post = service.update_post(post_id, update_data)
    if not post:
        raise HTTPException(status_code=404, detail="Post not found")
    return post


@router.delete("/{post_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_post(post_id: int, service: PostService = Depends(get_post_service)):
    """Delete a post"""
    success = service.delete_post(post_id)
    if not success:
        raise HTTPException(status_code=404, detail="Post not found")
    return Response(status_code=status.HTTP_204_NO_CONTENT)
