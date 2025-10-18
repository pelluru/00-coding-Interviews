from datetime import datetime, timedelta
import jwt

SECRET = "change-me"  # Use environment variable in production
ALGO = "HS256"

def create_token(sub: str, minutes: int = 60) -> str:
    payload = {"sub": sub, "exp": datetime.utcnow() + timedelta(minutes=minutes)}
    return jwt.encode(payload, SECRET, algorithm=ALGO)

def verify_token(token: str) -> str:
    data = jwt.decode(token, SECRET, algorithms=[ALGO])
    return data["sub"]