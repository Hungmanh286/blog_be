from typing import Optional
from datetime import datetime
from sqlmodel import SQLModel, Field
from sqlalchemy import Column, DateTime


class Post(SQLModel, table=True):
    __tablename__ = "posts"
    id: Optional[int] = Field(default=None, primary_key=True)
    category: str
    title: str
    date: str
    heroImage: str
    content: str
    area: str
    url: str
    published_at: Optional[datetime] = Field(
        default=None, sa_column=Column("published_at", DateTime, nullable=True)
    )
