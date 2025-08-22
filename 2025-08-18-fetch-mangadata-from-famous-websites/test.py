import requests

url = "https://www.anime-planet.com/manga/tower-of-god/recommendations"

response = requests.get(url)
html = response.text

with open("recommendations.html", "w", encoding="utf-8") as f:
    f.write(html)

print("Saved recommendations.html (length:", len(html), ")")