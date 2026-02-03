import os, requests, datetime, asyncio, json
from bs4 import BeautifulSoup
from google.cloud import firestore, storage
from google.oauth2 import service_account
from playwright.async_api import async_playwright

# Firebase setup - support both GitHub Actions and local development
if os.getenv("FIREBASE_KEY"):
    # Running in GitHub Actions or with environment variable
    firebase_credentials = json.loads(os.getenv("FIREBASE_KEY"))
    credentials = service_account.Credentials.from_service_account_info(firebase_credentials)
    print("Using Firebase credentials from FIREBASE_KEY environment variable")
else:
    # Running locally with credentials file
    firebase_key_path = os.path.join(os.path.dirname(__file__), "eco-guardian-bd74f-firebase-adminsdk-thlcj-b60714ed55.json")
    credentials = service_account.Credentials.from_service_account_file(firebase_key_path)
    print("Using Firebase credentials from local file")

db = firestore.Client(credentials=credentials)
storage_client = storage.Client(credentials=credentials)
bucket = storage_client.bucket("eco-guardian-bd74f.appspot.com")

ENJOYMENT_CATEGORIES = ["party","trip","tour","concert","festival","brunch"]

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
    if query:
        print(f"  ↳ Using existing venue: {scraped_venue['name']}")
        return query[0].id, False  # Return (venue_id, is_new)
    new_venue = {
        "name": scraped_venue["name"],
        "location": scraped_venue["location"],
        "description": scraped_venue.get("description", ""),
        "backgroundImageUrl": scraped_venue.get("backgroundImageUrl", ""),
        "latitude": scraped_venue.get("latitude"),
        "longitude": scraped_venue.get("longitude"),
        "categories": ["Other"],
        "ownerId": "scraped",
        "venueType": "recreation",
        "vibeRating": None,
        "weeklyPrograms": None,
        "todayImages": [],
        "createdAt": firestore.SERVER_TIMESTAMP,
        "isDeleted": False
    }
    venue_ref = venues_ref.add(new_venue)
    print(f"  ✓ Created new venue: {scraped_venue['name']} at {scraped_venue['location']}")
    return venue_ref[1].id, True  # Return (venue_id, is_new)

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
    today = datetime.datetime.now(datetime.UTC)
    delta = (date_obj - today).days
    return delta >= 0


async def scrape_site_with_playwright(url, selectors):
    print(f"\n--- Starting scrape for {url} ---")
    added_events = 0
    skipped_events = 0
    new_venues = 0
    
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            # Set user agent to avoid being blocked
            await page.set_extra_http_headers({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            })
            
            print(f"Loading page...")
            await page.goto(url, timeout=60000, wait_until="networkidle")
            
            # Wait for event cards to load
            try:
                await page.wait_for_selector(selectors["card"], timeout=30000)
            except Exception as e:
                print(f"  ⚠ Could not find event cards with selector '{selectors['card']}': {e}")
                # Try to continue anyway in case some elements loaded
            
            # Get all event cards
            cards = await page.query_selector_all(selectors["card"])
            print(f"Found {len(cards)} event cards")
            
            if len(cards) == 0:
                print(f"  ⚠ No events found - the page might have changed structure or requires different selectors")
                scrape_summary[url] = {"added_events": 0, "skipped": 0, "new_venues": 0, "error": "No event cards found"}
                await browser.close()
                return

            for idx, item in enumerate(cards, start=1):
                try:
                    link_el = await item.query_selector(selectors["title"])
                    event_url = await link_el.get_attribute("href") if link_el else None
                    event_name = await link_el.inner_text() if link_el else "Unknown"
                    event_name = event_name.strip()

                    venue_el = await item.query_selector(selectors["venue"])
                    venue_name = await venue_el.inner_text() if venue_el else "Unknown"
                    venue_name = venue_name.strip()
                    
                    location_el = await item.query_selector(selectors["location"])
                    location = await location_el.inner_text() if location_el else venue_name
                    location = location.strip()

                    # --- Date parsing ---
                    if "allevents.ug" in url:
                        # Use datetime attribute for AllEvents
                        date_el = await item.query_selector("time.tribe-events-calendar-list__event-datetime")
                        if date_el:
                            datetime_attr = await date_el.get_attribute("datetime")
                            if datetime_attr:
                                base_date = datetime.date.fromisoformat(datetime_attr)
                                time_el = await item.query_selector(selectors["time"])
                                time_str = await time_el.inner_text() if time_el else ""
                                time_str = time_str.strip()
                                try:
                                    time_obj = datetime.datetime.strptime(time_str, "%I:%M %p").time()
                                    date_obj = datetime.datetime.combine(base_date, time_obj, tzinfo=datetime.UTC)
                                except Exception:
                                    date_obj = datetime.datetime.combine(base_date, datetime.time(0,0), tzinfo=datetime.UTC)
                            else:
                                print(f"Could not parse AllEvents date for {event_name}")
                                skipped_events += 1
                                continue
                        else:
                            print(f"Could not find date element for {event_name}")
                            skipped_events += 1
                            continue

                    elif "evento.ug" in url:
                        # Parse Evento date format
                        date_el = await item.query_selector(selectors["date"])
                        date_str = await date_el.inner_text() if date_el else ""
                        date_str = date_str.strip()
                        clean_date = date_str.replace("st","").replace("nd","").replace("rd","").replace("th","").strip()

                        parsed = None
                        for fmt in ["%B %d @ %I:%M %p", "%d %b %Y %H:%M", "%d %b %Y %I:%M %p", "%B %d, %Y @ %I:%M %p"]:
                            try:
                                parsed = datetime.datetime.strptime(clean_date, fmt).replace(tzinfo=datetime.UTC)
                                break
                            except Exception:
                                continue
                        if not parsed:
                            print(f"Could not parse Evento date '{date_str}' for {event_name}")
                            skipped_events += 1
                            continue
                        date_obj = parsed

                    else:
                        print(f"Unknown site type for {url}")
                        skipped_events += 1
                        continue

                    poster_el = await item.query_selector(selectors["poster"])
                    poster = await poster_el.get_attribute("src") if poster_el else ""
                    
                    desc_el = await item.query_selector(selectors.get("desc", ""))
                    description = await desc_el.inner_text() if desc_el else ""
                    description = description.strip()

                    fee_el = await item.query_selector(selectors.get("fee", ""))
                    fee_text = await fee_el.inner_text() if fee_el else "Free"
                    fee_text = fee_text.strip()

                    entry_fees = []
                    if fee_text.lower().startswith("free") or not fee_text:
                        is_free = True
                        price_indicator = 0
                    else:
                        is_free = False
                        price_indicator = 1
                        entry_fees.append({"name": "General", "amount": fee_text})
                except Exception as e:
                    print(f"Skipping card #{idx} due to parse error: {e}")
                    skipped_events += 1
                    continue

                # --- Duplicate prevention ---
                if event_url:
                    # Ensure URL is absolute
                    if event_url.startswith("/"):
                        from urllib.parse import urljoin
                        event_url = urljoin(url, event_url)
                    
                    existing = db.collection("YoVibe").document("data").collection("events") \
                        .where("sourceUrl", "==", event_url).get()
                    if existing:
                        print(f"Skipped duplicate event: {event_name}")
                        skipped_events += 1
                        continue

                if not is_upcoming_event(date_obj):
                    print(f"Skipped past event: {event_name} | Date: {date_obj}")
                    skipped_events += 1
                    continue

                # Create or get venue
                venue_id, is_new_venue = get_or_create_venue({
                    "name": venue_name,
                    "location": location,
                    "description": f"Venue for {venue_name}",
                    "backgroundImageUrl": poster,
                    "latitude": None,
                    "longitude": None
                })
                if is_new_venue:
                    new_venues += 1

                event_doc = {
                    "name": event_name,
                    "date": date_obj,
                    "time": date_obj.strftime("%H:%M"),
                    "location": location,
                    "posterImageUrl": poster,
                    "artists": [],
                    "venueId": venue_id,
                    "venueName": venue_name,
                    "description": description,
                    "entryFees": entry_fees,
                    "isFreeEntry": is_free,
                    "priceIndicator": price_indicator,
                    "sourceUrl": event_url,
                    "createdAt": firestore.SERVER_TIMESTAMP,
                    "isDeleted": False
                }
                db.collection("YoVibe").document("data").collection("events").add(event_doc)
                print(f"Added event: {event_name} | Fee: {fee_text}")
                added_events += 1

            await browser.close()
            
    except Exception as e:
        print(f"Error scraping {url}: {e}")
        scrape_summary[url] = {"added_events": 0, "skipped": 0, "new_venues": 0, "error": str(e)}
        return

    scrape_summary[url] = {"added_events": added_events, "skipped": skipped_events, "new_venues": new_venues, "error": None}
    print(f"Finished scrape for {url}: added={added_events}, skipped={skipped_events}, new_venues={new_venues}")


def scrape_all_sites():
    try:
        asyncio.run(scrape_all_with_playwright())
    except Exception as e:
        print(f"Error running scraper: {e}")

async def scrape_all_with_playwright():
    sites = [
        {
            "url": "https://allevents.ug/events/",
            "selectors": {
                "card": "div.tribe-events-calendar-list__event-row",
                "title": "h3.tribe-events-calendar-list__event-title a",
                "venue": ".tribe-events-calendar-list__event-venue-title",
                "location": ".tribe-events-calendar-list__event-venue-address",
                "date": ".tribe-events-calendar-list__event-datetime .tribe-event-date-start",
                "time": ".tribe-events-calendar-list__event-datetime .tribe-event-time",
                "poster": ".tribe-events-calendar-list__event-featured-image",
                "desc": ".tribe-events-calendar-list__event-description p",
                "fee": ".tribe-events-c-small-cta__price"
            }
        },
        {
            "url": "https://evento.ug/events?eventtype=Music%20and%20Concerts",
            "selectors": {
                "card": "div.card.h-100.cardy",
                "title": "h6 a",
                "venue": ".location-info a:last-of-type",
                "location": ".location-info a:last-of-type",
                "date": ".location-info a:first-of-type",
                "time": ".location-info a:first-of-type",
                "poster": ".blog-img img",
                "desc": ".card-body p",
                "fee": ".amount"
            }
        }
    ]

    for site in sites:
        await scrape_site_with_playwright(site["url"], site["selectors"])

    try:
        await scrape_quicket()
    except Exception as e:
        print(f"Error running Quicket scraper: {e}")
        scrape_summary["https://www.quicket.co.ug/events/uganda"] = {
            "added_events": 0,
            "skipped": 0,
            "new_venues": 0,
            "error": str(e)
        }

async def scrape_quicket():
    url = "https://www.quicket.co.ug/events/uganda"
    print(f"\n--- Starting scrape for {url} ---")
    added_events = 0
    skipped_events = 0
    new_venues = 0
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.goto(url, timeout=60000)
            await page.wait_for_selector("li.l-event-item", timeout=30000)

            events = await page.query_selector_all("li.l-event-item")
            print(f"Found {len(events)} event cards on Quicket")

            for idx, item in enumerate(events, start=1):
                try:
                    link_el = await item.query_selector("a.l-event-item-wrapper")
                    event_url = await link_el.get_attribute("href") if link_el else None

                    title_el = await item.query_selector(".l-hit")
                    event_name = await title_el.inner_text() if title_el else "Unknown"

                    venue_el = await item.query_selector(".l-hit-venue")
                    venue_name = await venue_el.inner_text() if venue_el else "Unknown"
                    location = venue_name

                    date_el = await item.query_selector(".l-date-container .l-date:nth-of-type(1)")
                    date_str = await date_el.inner_text() if date_el else ""
                    time_el = await item.query_selector(".l-date-container .l-date:nth-of-type(2)")
                    time_str = await time_el.inner_text() if time_el else ""

                    poster_el = await item.query_selector(".l-event-image")
                    poster_url = await poster_el.get_attribute("src") if poster_el else ""

                    fee_el = await item.query_selector(".l-price, .price, .amount")
                    fee_text = await fee_el.inner_text() if fee_el else "Free"

                    entry_fees = []
                    if fee_text.lower().startswith("free"):
                        is_free = True
                        price_indicator = 0
                    else:
                        is_free = False
                        price_indicator = 1
                        entry_fees.append({"name": "General", "amount": fee_text})
                except Exception as e:
                    print(f"Skipping card #{idx} due to parse error: {e}")
                    skipped_events += 1
                    continue

                # --- Date parsing ---
                try:
                    clean_date = date_str.strip()
                    if clean_date.lower().startswith("runs from"):
                        clean_date = clean_date.replace("Runs from", "").strip()
                    clean_date = clean_date.replace("st","").replace("nd","").replace("rd","").replace("th","")

                    # Drop weekday if present (format: "Weekday, Month Day, Year")
                    parts = clean_date.split(",")
                    if len(parts) >= 3:
                        # Format: "Friday, December 12, 2025" -> join "December 12" + "2025"
                        clean_date = f"{parts[1].strip()} {parts[2].strip()}"
                    elif len(parts) == 2:
                        # Format might be "December 12, 2025" -> join both parts
                        clean_date = f"{parts[0].strip()} {parts[1].strip()}"

                    date_obj = datetime.datetime.strptime(clean_date, "%B %d %Y").replace(tzinfo=datetime.UTC)

                    if time_str:
                        try:
                            time_obj = datetime.datetime.strptime(time_str.strip(), "%H:%M").time()
                            date_obj = datetime.datetime.combine(date_obj.date(), time_obj, tzinfo=datetime.UTC)
                        except Exception:
                            pass
                except Exception as e:
                    print(f"Could not parse Quicket date '{date_str}': {e}")
                    skipped_events += 1
                    continue

                # Duplicate prevention
                if event_url:
                    existing = db.collection("YoVibe").document("data").collection("events") \
                        .where("sourceUrl", "==", event_url).get()
                    if existing:
                        print(f"Skipped duplicate event (already scraped): {event_name}")
                        skipped_events += 1
                        continue

                if not is_upcoming_event(date_obj):
                    print(f"Skipped past event: {event_name} | Date: {date_obj}")
                    skipped_events += 1
                    continue

                # Create or get venue
                venue_id, is_new_venue = get_or_create_venue({
                    "name": venue_name,
                    "location": location,
                    "description": f"Venue for {venue_name}",
                    "backgroundImageUrl": poster_url,
                    "latitude": None,
                    "longitude": None
                })
                if is_new_venue:
                    new_venues += 1

                event_doc = {
                    "name": event_name,
                    "date": date_obj,
                    "time": time_str,
                    "location": location,
                    "posterImageUrl": poster_url,
                    "artists": [],
                    "venueId": venue_id,
                    "venueName": venue_name,
                    "description": "",
                    "entryFees": entry_fees,
                    "isFreeEntry": is_free,
                    "priceIndicator": price_indicator,
                    "sourceUrl": event_url,
                    "createdAt": firestore.SERVER_TIMESTAMP,
                    "isDeleted": False
                }
                db.collection("YoVibe").document("data").collection("events").add(event_doc)
                print(f"Added event: {event_name} | Fee: {fee_text}")
                added_events += 1

            await browser.close()
    except Exception as e:
        print(f"Error scraping Quicket: {e}")
        scrape_summary[url] = {"added_events": 0, "skipped": 0, "new_venues": 0, "error": str(e)}
        return

    scrape_summary[url] = {"added_events": added_events, "skipped": skipped_events, "new_venues": new_venues, "error": None}
    print(f"Finished scrape for Quicket: added={added_events}, skipped={skipped_events}, new_venues={new_venues}")

if __name__=="__main__":
    scrape_all_sites()
    print("\n--- Scrape Summary ---")
    for site, stats in scrape_summary.items():
        print(f"Site: {site}")
        print(f"  Added events: {stats['added_events']}")
        print(f"  Skipped events: {stats['skipped']}")
        print(f"  New venues: {stats['new_venues']}")
        if stats['error']:
            print(f"  Error: {stats['error']}")
