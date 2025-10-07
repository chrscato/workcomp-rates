import requests

zip_code = "10001"  # NYC
url = f"https://geocoding-api.open-meteo.com/v1/search?name={zip_code}&count=1"
response = requests.get(url).json()

if "results" in response:
    lat = response["results"][0]["latitude"]
    lon = response["results"][0]["longitude"]
    name = response["results"][0]["name"]
    print(f"{name}: {lat}, {lon}")
else:
    print("No results found")
