from sqlalchemy import create_engine

engine = create_engine("postgresql://admin:admin123@localhost:5432/testdb")

try:
    with engine.connect() as conn:
        print("PostgreSQL connected successfully!")
except Exception as e:
    print("PostgreSQL connection failed:", e)
