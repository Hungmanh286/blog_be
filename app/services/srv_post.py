from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import select
from app.schemas.sche_post import Post


class PostService:
    def __init__(self, session: Session):
        self.session = session

    def create_post(self, post_data: Post) -> Post:
        self.session.add(post_data)
        self.session.commit()
        self.session.refresh(post_data)
        return post_data

    def get_post(self, post_id: int) -> Optional[Post]:
        statement = select(Post).where(Post.id == post_id)
        result = self.session.execute(statement).scalars().first()
        return result

    def get_posts(self) -> List[Post]:
        statement = select(Post)
        results = self.session.execute(statement).scalars().all()
        return results

    def update_post(self, post_id: int, post_data: dict) -> Optional[Post]:
        post = self.get_post(post_id)
        if not post:
            return None
        for key, value in post_data.items():
            setattr(post, key, value)
        self.session.add(post)
        self.session.commit()
        self.session.refresh(post)
        return post

    def delete_post(self, post_id: int) -> bool:
        post = self.get_post(post_id)
        if not post:
            return False
        self.session.delete(post)
        self.session.commit()
        return True
