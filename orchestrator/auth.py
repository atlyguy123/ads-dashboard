from flask import request, Response
from functools import wraps
import os

# Team credentials - store these as environment variables in production
TEAM_CREDENTIALS = {
    os.getenv('ADMIN_USERNAME', 'admin'): os.getenv('ADMIN_PASSWORD', 'secure-password'),
    os.getenv('TEAM_USERNAME', 'team'): os.getenv('TEAM_PASSWORD', 'team-password'),
    # Add more team members as needed
}

def check_auth(username, password):
    """Check if username/password combination is valid"""
    return username in TEAM_CREDENTIALS and TEAM_CREDENTIALS[username] == password

def authenticate():
    """Send 401 response that enables basic auth"""
    return Response(
        'Could not verify your access level for that URL.\n'
        'You have to login with proper credentials', 401,
        {'WWW-Authenticate': 'Basic realm="Team Dashboard"'})

def requires_auth(f):
    """Decorator that requires authentication"""
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated 