from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

engine = create_engine("mysql://root:020406@localhost:3306/fortunate")

db_session = sessionmaker(autocommit=True, autoflush=True, bind=engine)

Base = declarative_base()

Base.metadata.create_all(bind=engine)


def get_database():
    db = db_session()
    try:
        yield db
    finally:
        db.close()
