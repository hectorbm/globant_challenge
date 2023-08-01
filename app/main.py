from fastapi import FastAPI
from .router import router
from .database import Base, engine

app = FastAPI()

# Include the router from .router in the main app.
app.include_router(router)

# Create all declared schemas in the database
Base.metadata.create_all(bind=engine)