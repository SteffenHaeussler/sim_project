import httpx

text_pairs = [("Hello world!", "guten abend")]
text_embedding = "Hallo Welt"

response = httpx.post(
    "http://localhost:8000/embedding",
    json=text_embedding,
)

embedding = response.text
print(embedding)

response = httpx.post(
    "http://localhost:8000/ranking",
    json=text_pairs,
)

ranking = response.text
print(ranking)
