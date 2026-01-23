import os, requests, datetime
from bs4 import BeautifulSoup
from google.cloud import firestore, storage
from google.oauth2 import service_account

# Firebase setup
firebase_key = os.environ.get("FIREBASE_KEY")
credentials = service_account.Credentials.from_service_account_info(eval(firebase_key))
db = firestore.Client(credentials=credentials)
storage_client = storage.Client(credentials=credentials)
bucket = storage_client.bucket("eco-guardian-bd74f.appspot.com")

ENJOYMENT_CATEGORIES = ["party","trip","tour","concert","festival","brunch"]

# Summary tracker
scrape_summary = {}

def normalize_string(s): return s.strip().lower() if s else ""

def upload_image_to_storage(image_url, event_name):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    response = requests.get(image_url, headers=headers)
    blob_name = f"events/{event_name.replace(' ', '_')}/poster.jpg"
    blob = bucket.blob(blob_name)
    blob.upload_from_string(response.content, content_type="image/jpeg")
    blob.make_public()
    return blob.public_url

def get_or_create_venue(scraped_venue):
    venues_ref = db.collection("YoVibe").document("data").collection("venues")
    query = venues_ref.where("name","==",normalize_string(scraped_venue["name"])) \
                      .where("location","==",normalize_string(scraped_venue["location"])).get()
    if query: return query[0].id
    new_venue = {
        "name": scraped_venue["name"],
        "location": scraped_venue["location"],
        "description": scraped_venue.get("description",""),
        "latitude": scraped_venue.get("latitude"),
        "longitude": scraped_venue.get("longitude"),
        "createdAt": firestore.SERVER_TIMESTAMP,
        "isDeleted": False
    }
    venue_ref = venues_ref.add(new_venue)
    return venue_ref[1].id

def event_exists(event_name, date, venue_id):
    events_ref = db.collection("YoVibe").document("data").collection("events")
    query = events_ref.where("name","==",normalize_string(event_name)) \
                      .where("venueId","==",venue_id) \
                      .where("date","==",date).get()
    return len(query)>0

def is_enjoyment_event(event_name, description=""):
    text = f"{event_name} {description}".lower()
    return any(cat in text for cat in ENJOYMENT_CATEGORIES)

def is_upcoming_event(date_obj):
    today = datetime.datetime.utcnow()
    delta = (date_obj - today).days
    return 0 <= delta <= 30

def scrape_site(url, selectors):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
    except Exception as e:
        print(f"Skipping {url} due to error: {e}")
        scrape_summary[url] = {"added_events": 0, "skipped": 0, "error": str(e)}
        return

    added_events = 0
    skipped_events = 0

    soup = BeautifulSoup(response.text,"html.parser")
    for item in soup.select(selectors["card"]):
        event_name = item.select_one(selectors["title"]).text.strip()
        venue_name = item.select_one(selectors["venue"]).text.strip()
        location = item.select_one(selectors["location"]).text.strip()
        date_str = item.select_one(selectors["date"]).text.strip()
        time_str = item.select_one(selectors["time"]).text.strip()
        poster = item.select_one(selectors["poster"])["src"]
        description = item.select_one(selectors["desc"]).text.strip() if selectors.get("desc") else ""

        # Filter enjoyment events
        if not is_enjoyment_event(event_name, description):
            print(f"Skipped non-enjoyment event: {event_name}")
            skipped_events += 1
            continue

        # Parse date
        try: date_obj = datetime.datetime.strptime(date_str,"%d %B %Y")
        except: date_obj = datetime.datetime.utcnow()

        # Filter upcoming events (next 30 days)
        if not is_upcoming_event(date_obj):
            print(f"Skipped event outside 30-day window: {event_name}")
            skipped_events += 1
            continue

        # Venue handling
        venue_id = get_or_create_venue({"name":venue_name,"location":location})
        if event_exists(event_name,date_obj,venue_id):
            print(f"Skipped duplicate event: {event_name}")
            skipped_events += 1
            continue

        # Upload poster image
        poster_url = upload_image_to_storage(poster,event_name)

        # Event document
        event_doc = {
            "name": event_name,
            "date": date_obj,
            "time": time_str,
            "location": location,
            "posterImageUrl": poster_url,
            "artists": [],
            "venueId": venue_id,
            "venueName": venue_name,
            "description": description,
            "isFreeEntry": True,
            "createdAt": firestore.SERVER_TIMESTAMP,
            "isDeleted": False
        }
        db.collection("YoVibe").document("data").collection("events").add(event_doc)
        print(f"Added event: {event_name}")
        added_events += 1

    scrape_summary[url] = {
        "added_events": added_events,
        "skipped": skipped_events,
        "error": None
    }

def scrape_all_sites():
    sites = [
        {"url":"https://allevents.ug/events/",
         "selectors":{"card":".event-card","title":".event-title","venue":".event-venue",
                      "location":".event-location","date":".event-date","time":".event-time","poster":"img"}},
        {"url":"https://evento.ug/events?eventtype=Music%20and%20Concerts",
         "selectors":{"card":".event-item","title":".event-name","venue":".event-venue",
                      "location":".event-location","date":".event-date","time":".event-time","poster":"img"}},
        {"url":"https://www.quicket.co.ug/events/uganda",
         "selectors":{"card":".event-card","title":".event-title","venue":".event-venue",
                      "location":".event-location","date":".event-date","time":".event-time","poster":"img"}}
    ]
    for site in sites:
        scrape_site(site["url"],site["selectors"])

if __name__=="__main__":
    scrape_all_sites()
    print("\n--- Scrape Summary ---")
    for site, stats in scrape_summary.items():
        print(f"Site: {site}")
        print(f"  Added events: {stats['added_events']}")
        print(f"  Skipped events: {stats['skipped']}")
        if stats['error']:
            print(f"  Error: {stats['error']}")
