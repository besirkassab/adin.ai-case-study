from sqlalchemy import create_engine
from config_file import settings

def get_engine():
    db_url = f"mysql+pymysql://{settings.db.user}:{settings.db.password}@{settings.db.host}:{settings.db.port}/{settings.db.db}"
    engine = create_engine(db_url)
    return engine
