def authenticate(try_password: str, stored_password: str) -> bool:
    authenticated = False

    if not stored_password:
        # No password set, allow access
        authenticated = True
    elif try_password == stored_password:
        authenticated = True
    
    return authenticated