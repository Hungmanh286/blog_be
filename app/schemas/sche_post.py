from typing import Optional, Dict, Any
from datetime import datetime
from sqlmodel import SQLModel, Field
from sqlalchemy import Column, DateTime
from sqlalchemy.dialects.postgresql import JSONB


class Post(SQLModel, table=True):
    """Database model for posts"""

    __tablename__ = "posts"
    id: Optional[int] = Field(default=None, primary_key=True)
    category: str
    description: str
    title: str
    date: str
    heroImage: str
    content: Dict[Any, Any] = Field(sa_column=Column(JSONB))
    area: str
    url: str
    published_at: Optional[datetime] = Field(
        default=None, sa_column=Column("published_at", DateTime, nullable=True)
    )


class PostCreate(SQLModel):
    """Schema for creating a new post"""

    category: str
    description: str
    title: str
    date: str
    heroImage: str
    content: Dict[Any, Any]  # TipTap JSON content
    area: str
    url: str


class PostUpdate(SQLModel):
    """Schema for updating a post"""

    category: Optional[str] = None
    description: Optional[str] = None
    title: Optional[str] = None
    date: Optional[str] = None
    heroImage: Optional[str] = None
    content: Optional[Dict[Any, Any]] = None  # TipTap JSON content
    area: Optional[str] = None
    url: Optional[str] = None


class PostResponse(SQLModel):
    """Schema for post response"""

    id: int
    category: str
    description: str
    title: str
    date: str
    heroImage: str
    content: Dict[Any, Any]  # TipTap JSON content
    area: str
    url: str
    published_at: Optional[datetime] = None
