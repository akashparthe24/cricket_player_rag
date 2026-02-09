import os
import json
import time
import requests
from PIL import Image
from io import BytesIO
from tqdm import tqdm

# ---------------- CONFIG ----------------
JSON_PATH = "/Users/akashparthe/Desktop/Git Demo/Retrieval System/data/player_metadata.json"
IMAGE_DIR = "data/images"

REQUEST_DELAY = 2
TIMEOUT = 20

HEADERS = {
    "User-Agent": "Mozilla/5.0 (ImageCollector/1.0)",
    "Accept-Language": "en-US,en;q=0.9"
}

os.makedirs(IMAGE_DIR, exist_ok=True)

# ---------------- SAFE IMAGE DOWNLOAD ----------------
def download_image(url, save_path):
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        Image.open(BytesIO(r.content)).convert("RGB").save(save_path)
        return True
    except Exception:
        return False

# ---------------- MAIN ----------------
def main():
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        players = json.load(f)

    for player_name, data in tqdm(players.items(), desc="Downloading images"):
        if data.get("image_path"):
            continue  # already exists

        # 🔹 Image source priority:
        # 1. Explicit image_url (if you add later)
        # 2. ESPN Cricinfo player image CDN pattern
        # 3. Skip safely if not found

        image_url = data.get("image_url")

        if not image_url:
            # ESPN Cricinfo common CDN pattern (works for many players)
            slug = player_name.lower().replace(" ", "-")
            image_url = f"https://img1.hscicdn.com/image/upload/f_auto,q_auto/lsci/db/PICTURES/CMS/{slug}.jpg"

        image_filename = player_name.replace(" ", "_") + ".jpg"
        image_path = os.path.join(IMAGE_DIR, image_filename)

        success = download_image(image_url, image_path)

        if success:
            data["image_path"] = image_path
        else:
            data["image_path"] = ""

        time.sleep(REQUEST_DELAY)

    # ---------------- SAVE UPDATED JSON ----------------
    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(players, f, indent=2, ensure_ascii=False)

    print("✅ Image paths updated successfully!")

# ---------------- RUN ----------------
if __name__ == "__main__":
    main()
