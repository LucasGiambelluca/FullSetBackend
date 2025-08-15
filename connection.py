# connection.py
# Archivo de configuración de conexión a MariaDB usando SQLAlchemy y PyMySQL

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base

# URL de conexión: usuario, contraseña, host, puerto y base de datos
DATABASE_URL = (
    "mysql+pymysql://fullset:fullset@localhost:3306/fullset_db?charset=utf8mb4"
)

# Crear el engine de SQLAlchemy
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,       # verifica la conexión antes de usarla
    pool_recycle=3600         # reciclar conexiones inactivas cada hora
)

# Configurar la fábrica de sesiones
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

# Base para los modelos ORM
Base = declarative_base()

# Dependencia para FastAPI: obtener y cerrar sesión
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# Bloque de prueba de conexión
if __name__ == "__main__":
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            print("Conexión exitosa. SELECT 1 devuelve:", result.scalar())
    except Exception as e:
        print("Error de conexión a la base de datos:", e)
