import requests
from typing import List, Dict, Any
from src.config import Config

class APIClient:
    """
    Singleton client for handling external API interactions.
    Handles authentication and error management centrally.
    """
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(APIClient, cls).__new__(cls)
        return cls._instance

    def fetch_data(self, action_type: str) -> List[Dict[str, Any]]:
        """
        Retrieves raw JSON events for a specific action type.
        """
        try:
            response = requests.get(
                Config.API_URL,
                headers={"Authorization": f"Bearer {Config.API_KEY}"},
                params={"action": action_type},
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            if not data:
                return []
            return data
        except requests.exceptions.RequestException as e:
            print(f"[ERROR] API Request failed for {action_type}: {e}")
            return []