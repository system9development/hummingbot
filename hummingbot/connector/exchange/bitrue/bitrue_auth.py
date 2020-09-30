
class BitrueAuth():
    """
    Auth class for managing user credentials.
    """
    def __init__(self, api_key: str, secret_key: str):
        self.api_key = api_key
        self.secret_key = secret_key

    def get_auth_credentials(self):
        return {'api_key': self.api_key, 'api_secret': self.secret_key}
