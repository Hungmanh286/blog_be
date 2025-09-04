from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Mapped, mapped_column
from starlette.applications import Starlette

from starlette_admin.contrib.sqla import Admin, ModelView

Base = declarative_base()
engine = create_engine("postgresql+psycopg2://admin:123123@localhost:5436/blog")


# Define your model
class Post(Base):
    __tablename__ = "posts"

    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str]


Base.metadata.create_all(engine)

app = Starlette()

# Create admin
admin = Admin(engine, title="Example: SQLAlchemy")

# Add view
admin.add_view(ModelView(Post))

# Mount admin to your app
admin.mount_to(app)
