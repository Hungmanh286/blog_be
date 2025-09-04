from typing import List

from fastapi import APIRouter, Depends, HTTPException, Response, status

from app.db.base import Session, get_db
from app.schemas.sche_post import Post
from app.services.srv_post import PostService


def get_post_service(session: Session = Depends(get_db)) -> PostService:
    return PostService(session)


router = APIRouter(tags=["posts"])


@router.post("/", response_model=Post, status_code=status.HTTP_201_CREATED)
def create_post(payload: Post, service: PostService = Depends(get_post_service)):
    post = service.create_post(payload)
    return post


@router.get("/{post_id}", response_model=Post)
def read_post(post_id: int, service: PostService = Depends(get_post_service)):
    post = service.get_post(post_id)
    if not post:
        raise HTTPException(status_code=404, detail="Post not found")
    return post


@router.get("/", response_model=List[Post])
def read_posts(service: PostService = Depends(get_post_service)):
    return service.get_posts()


@router.put("/{post_id}", response_model=Post)
def update_post(
    post_id: int, payload: Post, service: PostService = Depends(get_post_service)
):
    update_data = payload.dict(exclude_unset=True)
    update_data.pop("id", None)
    post = service.update_post(post_id, update_data)
    if not post:
        raise HTTPException(status_code=404, detail="Post not found")
    return post


@router.delete("/{post_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_post(post_id: int, service: PostService = Depends(get_post_service)):
    success = service.delete_post(post_id)
    if not success:
        raise HTTPException(status_code=404, detail="Post not found")
    return Response(status_code=status.HTTP_204_NO_CONTENT)
