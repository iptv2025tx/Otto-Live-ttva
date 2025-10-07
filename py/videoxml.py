import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pytz
import sys
import xml.etree.ElementTree as ET
import time
import random
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Dictionary mapping channel IDs to channel names
channel_names = {
    "abc-wabc-new-york-ny/1769": "WABC (New York) ABC East",
    "cbs-wcbs-new-york-ny/1766": "WCBS (New York) CBS East",
    "nbc-wnbc-new-york-ny/1767": "WNBC (New York) NBC East",
    "fox-wnyw-new-york-ny-hd/4557": "WNYW (New York) FOX East",
    "ae-canada/4311": "A&E",
    "acc-network/33964": "ACC Network",
    "amc-canada/3822": "AMC",
    "american-heroes-channel/2035": "American Heroes Channel",
    "animal-planet-us-east/645": "Animal Planet",
    "bbc-america-east/615": "BBC America",
    "bbc-news-north-america/527": "BBC World News HD",
    "bet-eastern-feed/323": "BET",
    "bet-her/6837": "BET Her",
    "big-ten-network/4499": "Big Ten Network",
    "bloomberg-tv-usa-hd/6790": "Bloomberg TV",
    "boomerang/2268": "Boomerang",
    "bravo-usa-eastern-feed/646": "Bravo",
    "buzzr-tv-wwortv3-new-york-ny/10750": "Buzzr",
    "cartoon-network-usa-hd-eastern/6917": "Cartoon Network",
    "cbs-sports-network-usa/3115": "CBS Sports Network",
    "cinemax-eastern-feed/632": "Cinemax",
    "cnbc-usa/201": "CNBC",
    "cmt-canada/120": "CMT",
    "cmt-music/2288": "CMT Music",
    "cnn/70": "CNN",
    "cnn-international-north-america/3008": "CNN International",
    "comedy-central-us-eastern-feed/647": "Comedy Central",
    "the-cooking-channel/4226": "Cooking Channel",
    "crime-investigation-network-usa-hd/6216": "Crime & Investigation HD",
    "cspan/648": "CSPAN",
    "cspan-2/1050": "CSPAN 2",
    "destination-america/2074": "Destination America",
    "discovery-channel-us-eastern-feed/649": "Discovery",
    "discovery-family-channel/4225": "Discovery Family Channel",
    "discovery-life-channel/1273": "Discovery Life",
    "disney-eastern-feed/595": "Disney Channel (East)",
    "disney-junior-usa-hd-east/10523": "Disney Junior",
    "disney-xd-usa-eastern-feed/1053": "Disney XD",
    "e-entertainment-usa-eastern-feed/617": "E!",
    "espn/594": "ESPN",
    "espn2/650": "ESPN2",
    "espn-news/1527": "ESPNews",
    "espn-u/3331": "ESPNU",
    "food-network-usa-eastern-feed/1054": "Food Network",
    "fox-business/4656": "Fox Business Network",
    "fox-news/1083": "FOX News Channel",
    "fox-sports-1/668": "FOX Sports 1",
    "fox-sports-2/2114": "FOX Sports 2",
    "freeform-east-feed/1011": "Freeform",
    "fuse-tv-hd-eastern/6221": "Fuse HD",
    "fx-networks-east-coast-hd/6111": "FX",
    "fx-movie-channel/1308": "FX Movie",
    "fxx-usa-eastern/1952": "FXX",
    "fyi-usa-hd-eastern/6211": "FYI",
    "game-show-network-east/329": "Game Show Network",
    "golf-channel-canada/9900": "Golf Channel",
    "hallmark-channel-hd-eastern/6213": "Hallmark",
    "hallmark-family/32480": "Hallmark Drama HD",
    "hallmark-mystery-eastern/4453": "Hallmark Movies & Mysteries HD",
    "hbo-2-eastern-feed-hd/6313": "HBO 2 East",
    "hbo-comedy-east/629": "HBO Comedy HD",
    "hbo-eastern-feed/614": "HBO East",
    "hbo-family-eastern-feed/628": "HBO Family East",
    "hbo-signature-hbo-3-eastern-hd/7099": "HBO Signature",
    "hbo-zone-hd-east/7102": "HBO Zone HD",
    "hgtv-usa-eastern-feed/623": "HGTV",
    "history-channel-us-hd-east/4660": "History",
    "hln/425": "HLN",
    "independent-film-channel-us/1966": "IFC",
    "investigation-discovery-usa-eastern/2090": "Investigation Discovery",
    "ion-eastern-feed-hd/8534": "ION Television East HD",
    "law-crime-network/32823": "Law and Crime",
    "lifetime-network-us-eastern-feed/654": "Lifetime",
    "lifetime-movies-hd-east/4723": "LMN",
    "logo-east/2091": "Logo",
    "mlb-network/6178": "MLB Network",
    "moremax-eastern-hd/7097": "MoreMAX",
    "motor-trend-hd/12597": "MotorTrend HD",
    "moviemax-hd/7101": "MovieMAX",
    "msnbc-usa/655": "MSNBC",
    "mtv-usa-eastern-feed/656": "MTV",
    "national-geographic-wild/7537": "Nat Geo WILD",
    "national-geographic-us-hd-eastern/4436": "National Geographic",
    "nba-tv-usa/3116": "NBA TV",
    "newsmax-tv/16818": "Newsmax TV",
    "nfl-network/3349": "NFL Network",
    "nfl-redzone/6921": "NFL Red Zone",
    "nhl-network-usa/14156": "NHL Network",
    "nick-jr-hd/11444": "Nick Jr.",
    "nickelodeon-usa-east-feed/658": "Nickelodeon East",
    "nicktoons-hd-east/11445": "Nicktoons",
    "outdoor-channel-us/1086": "Outdoor Channel",
    "oprah-winfrey-network-usa-eastern/1159": "OWN",
    "oxygen-eastern-feed/659": "Oxygen True Crime",
    "pbs-wnet-new-york-ny/1774": "PBS 13 (WNET) New York",
    "reelzchannel/4175": "ReelzChannel",
    "science-hd/5828": "Science",
    "sec-network/13711": "SEC Network",
    "paramount-with-showtime-eastern-feed/665": "Showtime (E)",
    "showtime-2-eastern/1387": "SHOWTIME 2",
    "starz-eastern/583": "STARZ East",
    "sundancetv-usa-east-hd/8264": "SundanceTV HD",
    "syfy-eastern-feed/596": "SYFY",
    "tbs-east/61": "TBS",
    "turner-classic-movies-canada/2847": "TCM",
    "teennick-eastern/1954": "TeenNick",
    "telemundo-east-hd/33284": "Telemundo East",
    "the-tennis-channel/2269": "Tennis Channel",
    "wpix-new-york-superstation/63": "The CW (WPIX New York)",
    "tmc-hd-eastern/4352": "The Movie Channel East",
    "the-weather-channel/1526": "The Weather Channel",
    "tlc-usa-eastern/5005": "TLC",
    "tnt-eastern-feed/347": "TNT",
    "travel-us-east/662": "Travel Channel",
    "trutv-usa-eastern/333": "truTV",
    "tv-one-hd/7082": "TV One HD",
    "universal-kids-hd/8835": "Universal Kids",
    "univision-eastern-feed-hd/8136": "Univision East",
    "usa-network-east-feed/640": "USA Network",
    "vh1-eastern-feed/663": "VH1",
    "vice/624": "VICE",
    "w-wtn-east/64": "W Network",
    "we-hd/6220": "WE tv",
    "metv-network/16325": "MeTV",
    "metv-toons-wjlp2-new-jersey/15178": "MeTV Toons",
    "nfl-redzone/6921": "NFL RedZone",
    "pop-east/10165": "Pop",
    "revolt-tv/11301": "Revolt",
    "showtime-extreme-eastern/1615": "Showtime Extreme",
    "showtime-next-hd-eastern/7109": "Showtime Next",
    "showtime-women-eastern/2273": "Showtime Women",
    "starz-comedy-eastern/4223": "Starz Comedy",
    "starz-edge-hd-eastern/7089": "Starz Edge",
    "starz-encore-eastern/667": "Starz Encore",
    "starz-encore-action-eastern/2078": "Starz Encore Action",
    "starz-encore-classic-eastern/2080": "Starz Encore Classic",
    "starz-encore-westerns-eastern/1959": "Starz Encore Westerns",
    "tv-land-eastern/1252": "TV Land",
}

def get_cisession_with_timezone(tz="America/New_York", retries=5, delay=3):
    """
    Create a session with TVPassport and set the timezone.
    Retries until status_code == 200 or retries are exhausted.
    """
    session = requests.Session()
    session.get("https://www.tvpassport.com/", headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"
    })

    payload = {"timezone": tz}
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.tvpassport.com/my-passport/dashboard"
    }

    for attempt in range(1, retries + 1):
        try:
            response = session.post(
                "https://www.tvpassport.com/my-passport/dashboard/save_timezone",
                data=payload,
                headers=headers,
                timeout=10
            )

            if response.status_code == 200 and "cisession" in session.cookies:
                return session, session.cookies["cisession"]

        except requests.exceptions.RequestException:
            pass

        time.sleep(delay)

    return None, None

def create_session():
    """Create a requests session with retry logic and proper timezone."""
    session, _ = get_cisession_with_timezone()
    if session is None:
        # Fallback to regular session if timezone setting fails
        session = requests.Session()
    
    retry = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def scrape_tv_programming(channel_id, date, session=None):
    """Scrape TV programming data with retry logic and rate limiting."""
    if session is None:
        session = create_session()
        
    url = f"https://www.tvpassport.com/tv-listings/stations/{channel_id}/{date}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
    }
    cookies = {
        "cisession": "d86212bfc9056dc4f9b43c43e4139a5f11f2f719"
    }
    
    try:
        # Add a small random delay to avoid overwhelming the server
        time.sleep(1 + random.uniform(0, 1))
        
        response = session.get(url, headers=headers, cookies=cookies, timeout=30)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4xx, 5xx)
        
        soup = BeautifulSoup(response.content, "html.parser")
        programming_items = soup.select(".station-listings .list-group-item")

        if not programming_items:
            print(f"Warning: No programming data found for {channel_id} on {date}", file=sys.stderr)
            return None

        programming_data = []

        for item in programming_items:
            try:
                start_time = parse_start(item)
                if not start_time:
                    continue
                    
                duration = parse_duration(item) or 30  # Default to 30 minutes if duration is missing
                end_time = start_time + timedelta(minutes=duration)
                
                programming_data.append({
                    "title": parse_title(item) or "Unknown",
                    "sub_title": parse_sub_title(item) or "",
                    "description": parse_description(item) or "",
                    "icon": parse_icon(item) or "",
                    "category": parse_category(item) or [],
                    "rating": parse_rating(item),
                    "actors": parse_actors(item) or [],
                    "guest": parse_guest(item) or [],
                    "director": parse_director(item) or [],
                    "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "channel_id": channel_id
                })
            except Exception as e:
                print(f"Error parsing program for {channel_id}: {str(e)}", file=sys.stderr)
                continue

        return programming_data if programming_data else None
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {channel_id} on {date}: {str(e)}", file=sys.stderr)
        return None

def parse_description(item):
    return item.get("data-description")

def parse_icon(item):
    return item.get("data-showpicture")

def parse_title(item):
    show_name = item.get("data-showname")
    episode_title = item.get("data-episodetitle")
    if show_name == "Movie":
        return episode_title
    else:
        return show_name

def parse_sub_title(item):
    return item.get("data-episodetitle")

def parse_category(item):
    showtype = item.get("data-showtype")
    return showtype.split(", ") if showtype else []

def parse_actors(item):
    cast = item.get("data-cast")
    return cast.split(", ") if cast else []

def parse_director(item):
    director = item.get("data-director")
    return director.split(", ") if director else []

def parse_guest(item):
    guest = item.get("data-guest")
    return guest.split(", ") if guest else []

def parse_rating(item):
    rating = item.get("data-rating")
    return {"system": "MPA", "value": rating.replace("TV", "TV-")} if rating else None

def parse_start(item):
    time_str = item.get("data-st")
    if time_str:
        return pytz.timezone("America/New_York").localize(datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S"))
    else:
        return None

def parse_duration(item):
    duration_str = item.get("data-duration")
    return int(duration_str) if duration_str else None

def prettify(elem, level=0):
    """Add indentation to the XML element."""
    indent = "\n" + level * "    "  # Four spaces for each level
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = indent + "    "
        if not elem.tail or not elem.tail.strip():
            elem.tail = indent
        for subelem in elem:
            prettify(subelem, level + 1)
        if not elem.tail or not elem.tail.strip():
            elem.tail = indent
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = indent

def format_timezone_aware_datetime(dt):
    if dt.tzinfo is None:
        return dt.strftime("%Y%m%d%H%M%S")
    else:
        return dt.strftime("%Y%m%d%H%M%S %z")

def sanitize_text(text):
    """Ensure text is properly encoded and safe for XML"""
    if text is None:
        return ""
    if not isinstance(text, str):
        text = str(text)
    # Replace any control characters except newlines and tabs
    return ''.join(c if ord(c) >= 32 or c in '\n\r\t' else ' ' for c in text)

def create_xml(programs):
    root = ET.Element("tv")
    
    # First pass: Add all channels
    channels_added = set()
    for channel_id, channel_programs in programs.items():
        channel_name = channel_names.get(channel_id, channel_id)
        if not channel_name:
            channel_name = str(channel_id)  # Fallback to channel_id if name is empty
            
        if channel_name not in channels_added:
            try:
                channel_elem = ET.SubElement(root, "channel", id=sanitize_text(channel_name))
                display_name_elem = ET.SubElement(channel_elem, "display-name")
                display_name_elem.set("lang", "en")
                display_name_elem.text = sanitize_text(channel_name)
                channels_added.add(channel_name)
            except Exception as e:
                print(f"Error adding channel {channel_name}: {e}", file=sys.stderr)
                continue
    
    # Second pass: Add all programs
    for channel_id, channel_programs in programs.items():
        channel_name = channel_names.get(channel_id, channel_id)
        if not channel_name:
            channel_name = str(channel_id)
            
        for program in channel_programs:
            try:
                program_elem = ET.SubElement(root, "programme")
                
                # Format times with error handling
                try:
                    start_time = program.get("start_time")
                    end_time = program.get("end_time")
                    if not (start_time and end_time):
                        raise ValueError("Missing start_time or end_time")
                        
                    program_elem.set("start", format_timezone_aware_datetime(start_time))
                    program_elem.set("stop", format_timezone_aware_datetime(end_time))
                except Exception as e:
                    print(f"Error with program times: {e}", file=sys.stderr)
                    root.remove(program_elem)
                    continue
                
                program_elem.set("channel", sanitize_text(channel_name))
                
                # Helper function to add text elements
                def add_text_element(parent, tag, text, attrs=None):
                    if not text:
                        return
                    elem = ET.SubElement(parent, tag)
                    if attrs:
                        for k, v in attrs.items():
                            elem.set(k, sanitize_text(v))
                    elem.text = sanitize_text(text)
                    return elem
                
                # Add program details
                add_text_element(program_elem, "title", program.get("title"), {"lang": "en"})
                add_text_element(program_elem, "sub-title", program.get("sub_title"), {"lang": "en"})
                add_text_element(program_elem, "desc", program.get("description"), {"lang": "en"})
                
                # Add categories
                categories = program.get("category", [])
                if not isinstance(categories, (list, tuple)):
                    categories = [categories] if categories else []
                for category in categories:
                    if category:
                        add_text_element(program_elem, "category", category, {"lang": "en"})
                
                # Add icon if available
                if program.get("icon"):
                    icon_elem = ET.SubElement(program_elem, "icon")
                    icon_elem.set("src", sanitize_text(program["icon"]))
                
                # Add rating if available
                if program.get("rating"):
                    rating = program["rating"]
                    if isinstance(rating, dict):
                        rating_elem = ET.SubElement(program_elem, "rating")
                        add_text_element(rating_elem, "system", rating.get("system", ""))
                        add_text_element(rating_elem, "value", rating.get("value", ""))
                
            except Exception as e:
                print(f"Error creating program element: {e}", file=sys.stderr)
                if program_elem in root:
                    root.remove(program_elem)
                continue
    
    # Apply indentation
    prettify(root)
    
    # Convert tree to XML string with proper encoding
    try:
        xml_str = ET.tostring(root, encoding="UTF-8", xml_declaration=True).decode("UTF-8")
        # Ensure the XML is valid by parsing it
        ET.fromstring(xml_str)
        return xml_str
    except Exception as e:
        print(f"Error generating XML: {e}", file=sys.stderr)
        # Fallback to a minimal valid XML if generation fails
        return '<?xml version="1.0" encoding="UTF-8"?>\n<tv>\n</tv>'

# Example usage
channel_ids = [
    "ae-canada/4311",
    "abc-wabc-new-york-ny/1769",
    "cbs-wcbs-new-york-ny/1766",
    "nbc-wnbc-new-york-ny/1767",
    "fox-wnyw-new-york-ny-hd/4557",
    "acc-network/33964",
    "amc-canada/3822",
    "american-heroes-channel/2035",
    "animal-planet-us-east/645",
    "bbc-america-east/615",
    "bbc-news-north-america/527",
    "bet-eastern-feed/323",
    "bet-her/6837",
    "big-ten-network/4499",
    "bloomberg-tv-usa-hd/6790",
    "boomerang/2268",
    "bravo-usa-eastern-feed/646",
    "bravo-canada/160",
    "buzzr-tv-wwortv3-new-york-ny/10750",
    "cartoon-network-usa-hd-eastern/6917",
    "cbs-sports-network-usa/3115",
    "cinemax-eastern-feed/632",
    "cmt-canada/120",
    "cmt-music/2288",
    "cnbc-usa/201",
    "cnn/70",
    "cnn-international-north-america/3008",
    "comedy-central-us-eastern-feed/647",
    "the-cooking-channel/4226",
    "crime-investigation-network-usa-hd/6216",
    "cspan/648",
    "cspan-2/1050",
    "destination-america/2074",
    "discovery-channel-us-eastern-feed/649",
    "discovery-family-channel/4225",
    "discovery-life-channel/1273",
    "disney-eastern-feed/595",
    "disney-junior-usa-hd-east/10523",
    "disney-xd-usa-eastern-feed/1053",
    "e-entertainment-usa-eastern-feed/617",
    "espn/594",
    "espn2/650",
    "espn-news/1527",
    "espn-u/3331",
    "food-network-usa-eastern-feed/1054",
    "fox-business/4656",
    "fox-news/1083",
    "fox-sports-1/668",
    "fox-sports-2/2114",
    "freeform-east-feed/1011",
    "fuse-tv-hd-eastern/6221",
    "fx-networks-east-coast-hd/6111",
    "fx-movie-channel/1308",
    "fxx-usa-eastern/1952",
    "fyi-usa-hd-eastern/6211",
    "golf-channel-canada/9900",
    "game-show-network-east/329",
    "hallmark-channel-hd-eastern/6213",
    "hallmark-family/32480",
    "hallmark-mystery-eastern/4453",
    "hbo-2-eastern-feed-hd/6313",
    "hbo-comedy-east/629",
    "hbo-eastern-feed/614",
    "hbo-family-eastern-feed/628",
    "hbo-signature-hbo-3-eastern-hd/7099",
    "hbo-zone-hd-east/7102",
    "hgtv-usa-eastern-feed/623",
    "history-channel-us-hd-east/4660",
    "hln/425",
    "independent-film-channel-us/1966",
    "investigation-discovery-usa-eastern/2090",
    "ion-eastern-feed-hd/8534",
    "law-crime-network/32823",
    "lifetime-network-us-eastern-feed/654",
    "lifetime-movies-hd-east/4723",
    "logo-east/2091",
    "max/306",
    "metv-network/16325",
    "mlb-network/6178",
    "moremax-eastern-hd/7097",
    "motor-trend-hd/12597",
    "moviemax-hd/7101",
    "msnbc-usa/655",
    "mtv-usa-eastern-feed/656",
    "national-geographic-wild/7537",
    "national-geographic-us-hd-eastern/4436",
    "nba-tv-usa/3116",
    "newsmax-tv/16818",
    "nfl-network/3349",
    "nfl-redzone/6921",
    "nhl-network-usa/14156",
    "nick-jr-hd/11444",
    "nickelodeon-usa-east-feed/658",
    "nicktoons-hd-east/11445",
    "outdoor-channel-us/1086",
    "oprah-winfrey-network-usa-eastern/1159",
    "oxygen-eastern-feed/659",
    "pbs-wnet-new-york-ny/1774",
    "planete-hd/6765",
    "reelzchannel/4175",
    "science-hd/5828",
    "sec-network/13711",
    "paramount-with-showtime-eastern-feed/665",
    "showtime-2-eastern/1387",
    "starz-eastern/583",
    "sundancetv-usa-east-hd/8264",
    "syfy-eastern-feed/596",
    "tbs-east/61",
    "turner-classic-movies-canada/2847",
    "teennick-eastern/1954",
    "telemundo-east-hd/33284",
    "the-tennis-channel/2269",
    "wpix-new-york-superstation/63",
    "tmc-hd-eastern/4352",
    "the-weather-channel/1526",
    "tlc-usa-eastern/5005",
    "tnt-eastern-feed/347",
    "travel-us-east/662",
    "trutv-usa-eastern/333",
    "tv-one-hd/7082",
    "universal-kids-hd/8835",
    "univision-eastern-feed-hd/8136",
    "usa-network-east-feed/640",
    "vh1-eastern-feed/663",
    "vice/624",
    "w-wtn-east/64",
    "we-hd/6220",
    "magnolia-network-canada/2034",
    "metv-network/16325",
    "metv-toons-wjlp2-new-jersey/15178",
    "mgm-east/7609",
    "mgm-drivein/11487",
    "mgm-hits-east/11485",
    "mgm-marquee/11486",
    "mtv-classic-east/2093",
    "nfl-redzone/6921",
    "nickmusic/2112",
    "paramount-network-usa-eastern-feed/1030",
    "pbs-kids-wkle4-lexington-ky/32103",
    "pop-east/10165",
    "revolt-tv/11301",
    "showtime-extreme-eastern/1615",
    "showtime-next-hd-eastern/7109",
    "showtime-women-eastern/2273",
    "starz-comedy-eastern/4223",
    "starz-edge-hd-eastern/7089",
    "starz-encore-eastern/667",
    "starz-encore-action-eastern/2078",
    "starz-encore-classic-eastern/2080",
    "starz-encore-westerns-eastern/1959",
    "tv-land-eastern/1252"
    ]

def main():
    # Create a single session to reuse connections
    session = create_session()
    
    # Calculate today's date and the dates for the next three days
    dates = [(datetime.now(pytz.timezone("America/New_York")) + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(4)]
    
    all_programs = {}
    failed_channels = set()
    
    # Process channels in a random order to distribute load
    random.shuffle(channel_ids)
    
    for i, channel_id in enumerate(channel_ids):
        print(f"Processing channel {i+1}/{len(channel_ids)}: {channel_names.get(channel_id, channel_id)}", file=sys.stderr)
        
        program_data = []
        channel_success = False
        
        for date in dates:
            try:
                program_data_for_date = scrape_tv_programming(channel_id, date, session)
                if program_data_for_date:
                    program_data.extend(program_data_for_date)
                    channel_success = True
            except Exception as e:
                print(f"Unexpected error processing {channel_id} on {date}: {str(e)}", file=sys.stderr)
        
        if program_data:
            all_programs[channel_id] = program_data
        elif not channel_success:
            failed_channels.add(channel_id)
    
    # Print summary
    if failed_channels:
        print(f"\nFailed to retrieve data for {len(failed_channels)} channels:", file=sys.stderr)
        for channel_id in sorted(failed_channels):
            print(f"- {channel_names.get(channel_id, channel_id)}", file=sys.stderr)
    
    # Process the programs
    processed_programs = {}
    for channel_id, programs in all_programs.items():
        if programs:  # Only process channels with programs
            processed_programs[channel_id] = []
            for program in programs:
                try:
                    # Convert string times to timezone-aware datetime objects
                    if isinstance(program['start_time'], str):
                        start_time = pytz.timezone("America/New_York").localize(
                            datetime.strptime(program['start_time'], "%Y-%m-%d %H:%M:%S")
                        )
                    else:
                        start_time = program['start_time']
                        if not start_time.tzinfo:
                            start_time = pytz.timezone("America/New_York").localize(start_time)
                            
                    # Calculate end time
                    duration = program.get('duration', 30)  # Default to 30 minutes if not present
                    if isinstance(program.get('end_time'), str):
                        end_time = pytz.timezone("America/New_York").localize(
                            datetime.strptime(program['end_time'], "%Y-%m-%d %H:%M:%S")
                        )
                    elif 'end_time' in program and program['end_time']:
                        end_time = program['end_time']
                        if not end_time.tzinfo:
                            end_time = pytz.timezone("America/New_York").localize(end_time)
                    else:
                        end_time = start_time + timedelta(minutes=duration)
                    
                    # Prepare program data for XML generation
                    processed_program = {
                        "title": program.get('title', 'Unknown Program'),
                        "sub_title": program.get('sub_title', ''),
                        "description": program.get('description', ''),
                        "category": program.get('category', []),
                        "icon": program.get('icon', ''),
                        "rating": program.get('rating'),
                        "actors": program.get('actors', []),
                        "guest": program.get('guest', []),
                        "director": program.get('director', ''),
                        "start_time": start_time,
                        "end_time": end_time,
                        "channel_id": channel_id
                    }
                    processed_programs[channel_id].append(processed_program)
                        
                except Exception as e:
                    print(f"Error processing program data for {channel_id}: {str(e)}", file=sys.stderr)
                    continue
        
    # Print debug info
    total_channels = len(processed_programs)
    total_programs = sum(len(progs) for progs in processed_programs.values())
    print(f"\nSuccessfully processed {total_programs} programs from {total_channels} channels", file=sys.stderr)
    
    # Generate and print the XML
    xml_content = create_xml(processed_programs)
    print(xml_content)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nScript interrupted by user.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"\nFatal error: {str(e)}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
