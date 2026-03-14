from typing import List, Optional, Dict
from sqlalchemy.orm import Session
from sqlalchemy import select, or_, cast, String
from app.schemas.sche_post import Post, PostCreate


class PostService:
    def __init__(self, session: Session):
        self.session = session

    def create_post(self, post_data: PostCreate) -> Post:
        """Create a new post from PostCreate schema"""
        post = Post(**post_data.dict())
        self.session.add(post)
        self.session.commit()
        self.session.refresh(post)
        return post

    def get_post(self, post_id: int) -> Optional[Post]:
        statement = select(Post).where(Post.id == post_id)
        result = self.session.execute(statement).scalars().first()
        return result

    def get_areas(self) -> List[str]:
        statement = (
            select(Post.area).distinct().where(Post.area != None).order_by(Post.area)
        )
        return list(self.session.execute(statement).scalars().all())

    def get_categories(self) -> List[str]:
        statement = (
            select(Post.category)
            .distinct()
            .where(Post.category != None)
            .order_by(Post.category)
        )
        return list(self.session.execute(statement).scalars().all())

    def get_archives(self) -> List[Dict[str, int]]:
        statement = select(Post.date).distinct().where(Post.date != None)
        dates = self.session.execute(statement).scalars().all()
        archives = set()
        for d in dates:
            if d and len(d) >= 7:
                try:
                    year = int(d[:4])
                    month = int(d[5:7])
                    archives.add((year, month))
                except ValueError:
                    continue

        # Sort by year desc, month desc
        return [{"year": y, "month": m} for y, m in sorted(archives, reverse=True)]

    def get_posts(
        self,
        area: Optional[str] = None,
        category: Optional[str] = None,
        month: Optional[int] = None,
        year: Optional[int] = None,
        q: Optional[str] = None,
    ) -> List[Post]:
        statement = select(Post)

        if q:
            search_pattern = f"%{q}%"
            statement = statement.where(
                or_(
                    Post.title.ilike(search_pattern),
                    Post.description.ilike(search_pattern),
                    cast(Post.content, String).ilike(search_pattern),
                )
            )

        if area:
            statement = statement.where(Post.area == area)
        if category:
            statement = statement.where(Post.category == category)

        if year and month:
            prefix = f"{year}-{month:02d}-"
            statement = statement.where(Post.date.startswith(prefix))
        elif year:
            prefix = f"{year}-"
            statement = statement.where(Post.date.startswith(prefix))
        elif month:
            # If only month is provided, we might need a regex or like '____-MM-%'.
            # A simple like:
            statement = statement.where(Post.date.like(f"%-{month:02d}-%"))

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
