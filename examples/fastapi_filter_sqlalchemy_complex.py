import logging
import random
from typing import Any, AsyncIterator, List, Optional

import click
import uvicorn
from faker import Faker
from fastapi import Depends, FastAPI, Query
from pydantic import BaseModel, Field
from sqlalchemy import Column, ForeignKey, Integer, String, event, select, Table
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Mapped, declarative_base, relationship

from fastapi_filter import FilterDepends, with_prefix
from fastapi_filter.contrib.sqlalchemy import Filter

logger = logging.getLogger("uvicorn")


@event.listens_for(Engine, "connect")
def _set_sqlite_case_sensitive_pragma(dbapi_con, connection_record):
    cursor = dbapi_con.cursor()
    cursor.execute("PRAGMA case_sensitive_like=ON;")
    cursor.close()


engine = create_async_engine("sqlite+aiosqlite:///fastapi_filter.sqlite")
async_session = async_sessionmaker(engine, class_=AsyncSession)

Base = declarative_base()

fake = Faker()

address_association_table = Table(
    "address_association_table",
    Base.metadata,
    Column("userd_id", ForeignKey("users.id"), primary_key=True),
    Column("address_id", ForeignKey("addresses.id"), primary_key=True),
)

jobs_association_table = Table(
    "jobs_association_table",
    Base.metadata,
    Column("user_id", ForeignKey("users.id"), primary_key=True),
    Column("job_id", ForeignKey("jobs.id"), primary_key=True),
)


class Jobs(Base):
    __tablename__ = "jobs"
    id = Column(Integer, primary_key=True, autoincrement=True)
    description = Column(String, nullable=False)
    pay = Column(Integer, nullable=False)
    users: Mapped[List["User"]] = relationship(secondary=jobs_association_table, back_populates="jobs", lazy="selectin")


class Address(Base):
    __tablename__ = "addresses"

    id = Column(Integer, primary_key=True, autoincrement=True)
    street = Column(String, nullable=False)
    city = Column(String, nullable=False)
    country = Column(String, nullable=False)
    users: Mapped[List["User"]] = relationship(
        secondary=address_association_table, back_populates="addresses", lazy="selectin"
    )


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    email = Column(String, nullable=False)
    age = Column(Integer, nullable=False)
    addresses: Mapped[List["Address"]] = relationship(
        secondary=address_association_table, back_populates="users", lazy="selectin"
    )
    jobs: Mapped[List["Jobs"]] = relationship(secondary=jobs_association_table, back_populates="users", lazy="selectin")


class JobOut(BaseModel):
    id: int
    description: str
    pay: int

    class Config:
        orm_mode = True


class UserIn(BaseModel):
    name: str
    email: str
    age: int


class UserOut(UserIn):
    id: int
    jobs: Optional[List[JobOut]]

    class Config:
        orm_mode = True


class AddressOut(BaseModel):
    id: int
    street: str
    city: str
    country: str
    users: List[UserOut]

    class Config:
        orm_mode = True


class JobFilter(Filter):
    description__ilike: Optional[str] = None
    pay__gte: Optional[int] = None
    pay__lte: Optional[int] = None
    pay__or__between: Optional[List[List[int]]] = None
    pay__and__between: Optional[List[List[int]]] = None

    class Constants(Filter.Constants):
        model = Jobs


class UserFilter(Filter):
    name: Optional[str] = None
    name__ilike: Optional[str] = None
    name__like: Optional[str] = None
    name__neq: Optional[str] = None
    age__lt: Optional[int] = None
    job: Optional[JobFilter] = FilterDepends(with_prefix("job", JobFilter))

    class Constants(Filter.Constants):
        model = User


class AddressFilter(Filter):
    street: Optional[str] = None
    country: Optional[str] = None
    city__in: Optional[List[str]] = None
    users: Optional[UserFilter] = FilterDepends(with_prefix("users", UserFilter))

    class Constants(Filter.Constants):
        model = Address


app = FastAPI()


@app.on_event("startup")
async def on_startup() -> None:
    message = "Open http://127.0.0.1:8000/docs to start exploring 🎒 🧭 🗺️"
    color_message = "Open " + click.style("http://127.0.0.1:8000/docs", bold=True) + " to start exploring 🎒 🧭 🗺️"
    logger.info(message, extra={"color_message": color_message})

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    async with async_session() as session:
        addresses = []
        jobs = []
        for _ in range(100):
            address = Address(street=fake.street_address(), city=fake.city(), country=fake.country())
            addresses.append(address)

        for _ in range(100):
            job = Jobs(description=fake.job(), pay=fake.random_int(min=5000, max=120000))
            jobs.append(job)

        for _ in range(100):
            user = User(
                name=fake.name(),
                email=fake.email(),
                age=fake.random_int(min=5, max=120),
                addresses=random.sample(addresses, random.sample(range(3), 1)[0]),
                jobs=random.sample(jobs, random.sample(range(3), 1)[0]),
            )

            session.add_all([address, user])
        await session.commit()


@app.on_event("shutdown")
async def on_shutdown() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


async def get_db() -> AsyncIterator[AsyncSession]:
    async with async_session() as session:
        yield session


@app.get("/users", response_model=List[UserOut])
async def get_users(
    user_filter: UserFilter = FilterDepends(UserFilter),
    db: AsyncSession = Depends(get_db),
) -> Any:
    query = select(User).join(User.addresses).join(User.jobs)
    query = user_filter.filter(query)
    result = await db.execute(query)
    return result.scalars().all()


@app.get("/addresses", response_model=List[AddressOut])
async def get_addresses(
    address_filter: AddressFilter = FilterDepends(AddressFilter),
    db: AsyncSession = Depends(get_db),
) -> Any:
    query = select(Address).join(Address.users)
    query = address_filter.filter(query)
    result = await db.execute(query)
    return result.scalars().all()


if __name__ == "__main__":
    uvicorn.run("fastapi_filter_sqlalchemy_complex:app", reload=True)
