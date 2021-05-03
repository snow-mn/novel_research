import requests

payload = {"out": "json", "min_globalpoint":100}
r = requests.get("https://api.syosetu.com/novelapi/api/", params=payload)
x = r.json()

keyword = "日常"

for n in x:
    if len(n) > 1:
        metadata = [n["ncode"], n["title"], n["genre"], n["keyword"], n["story"]]
        keywords = metadata[3].split(" ")
        if keyword in keywords:
            print(metadata)