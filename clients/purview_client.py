import requests


class PurviewClient:

    def __init__(self, account, token):

        self.base_url = f"https://{account}.purview.azure.com"
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

    def get(self, endpoint):

        url = f"{self.base_url}{endpoint}"

        response = requests.get(url, headers=self.headers)
        response.raise_for_status()

        return response.json()

    def post(self, endpoint, payload):

        url = f"{self.base_url}{endpoint}"

        response = requests.post(
            url,
            headers=self.headers,
            json=payload
        )

        response.raise_for_status()

        return response.json()

    def put(self, endpoint, payload):

        url = f"{self.base_url}{endpoint}"

        response = requests.put(
            url,
            headers=self.headers,
            json=payload
        )

        response.raise_for_status()

        return response.json()
