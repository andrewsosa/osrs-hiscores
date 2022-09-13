import datetime

from sqlalchemy import Column, ForeignKey, UniqueConstraint, create_engine, types
from sqlalchemy.orm import Mapped, Session, declarative_base, relationship

Base = declarative_base()


def make_engine():
    return create_engine("postgresql://postgres:postgres@localhost/")


class Page(Base):
    __tablename__ = "page"
    __table_args__ = (UniqueConstraint("skill_name", "page_number", "datestamp"),)

    id: Mapped[int] = Column(types.Integer(), autoincrement=True, primary_key=True)

    skill_name: Mapped[str] = Column(types.String(), index=True, nullable=False)
    page_number: Mapped[int] = Column(types.Integer(), index=True, nullable=False)
    datestamp: Mapped[datetime.date] = Column(types.Date(), index=True, nullable=False)


class Score(Base):
    __tablename__ = "score"

    id: Mapped[int] = Column(types.Integer(), autoincrement=True, primary_key=True)
    page_id: Mapped[int] = Column(
        types.Integer(),
        ForeignKey("page.id"),
        index=True,
        nullable=False,
    )

    skill_name: Mapped[str] = Column(types.String(), index=True, nullable=False)
    player_name: Mapped[str] = Column(types.String(), index=True, nullable=False)
    rank: Mapped[int] = Column(types.Integer(), nullable=False)
    level: Mapped[int] = Column(types.Integer(), nullable=False)
    xp: Mapped[int] = Column(types.Integer(), nullable=False)
    timestamp: Mapped[datetime.datetime] = Column(types.DateTime(), nullable=False)

    page: Mapped[Page] = relationship("Page", uselist=False)
