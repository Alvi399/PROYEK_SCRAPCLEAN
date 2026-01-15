import pandas as pd
import random
import logging
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium.webdriver.common.action_chains import ActionChains
import os
import csv
import sys
import re
import glob
from urllib.parse import quote

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def init_driver(headless=True, timeout=20):
    """Create and return a single Chrome WebDriver with safe options."""
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    
    # === OPTIMASI KECEPATAN ===
    options.add_argument("--blink-settings=imagesEnabled=false")  # Disable images
    options.add_argument("--disable-extensions")  # Disable extensions
    options.add_argument("--disable-plugins")  # Disable plugins
    options.add_argument("--disable-infobars")  # Disable infobars
    options.add_argument("--disable-notifications")  # Disable notifications
    options.add_argument("--disable-popup-blocking")  # Disable popup blocking
    options.add_argument("--disable-translate")  # Disable translate
    options.add_argument("--disable-features=TranslateUI")  # Disable translate UI
    options.add_argument("--disable-ipc-flooding-protection")  # Faster IPC
    options.add_argument("--disable-renderer-backgrounding")  # Keep renderer active
    options.page_load_strategy = 'eager'  # Don't wait for all resources to load
    
    # === ANTI-BOT DETECTION ===
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(options=options, service=Service(ChromeDriverManager().install()))
    try:
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => false});")
    except Exception:
        pass
    driver.set_page_load_timeout(timeout)
    return driver


def extract_coords_from_url(url):
    """Extract latitude and longitude from common Google Maps URL patterns."""
    try:
        m = re.search(r"@(-?\d+\.\d+),(-?\d+\.\d+)", url)
        if m:
            return m.group(1), m.group(2)
        m = re.search(r"3d(-?\d+\.\d+)!4d(-?\d+\.\d+)", url)
        if m:
            return m.group(1), m.group(2)
        m = re.search(r"/@(-?\d+\.\d+),(-?\d+\.\d+)", url)
        if m:
            return m.group(1), m.group(2)
    except Exception:
        pass
    return None, None


def wait_for_coords_in_url(driver, max_wait=10):
    """Wait for coordinates to appear in the URL (up to max_wait seconds)."""
    start = time.time()
    while time.time() - start < max_wait:
        url = driver.current_url
        lat, lon = extract_coords_from_url(url)
        if lat and lon:
            return lat, lon
        time.sleep(0.5)
    return extract_coords_from_url(driver.current_url)


def extract_coords_from_page(driver):
    """Fallback: Try to extract coordinates from the info panel or share link on the page."""
    try:
        share_button = driver.find_element(By.XPATH, "//*[contains(@aria-label, 'Share') or contains(@aria-label, 'Bagikan')]")
        driver.execute_script("arguments[0].click();", share_button)
        time.sleep(0.8)
        try:
            share_link = driver.find_element(By.XPATH, "//input[@value]")
            url = share_link.get_attribute("value")
            lat, lon = extract_coords_from_url(url)
            if lat and lon:
                return lat, lon
        except Exception:
            pass
    except Exception:
        pass
    return None, None

# Fungsi untuk mendapatkan informasi dari Google Maps berdasarkan nama tempat
def get_place_status(driver):
    """Infer place status (Aktif/Tutup Permanen/Tutup Sementara) from page content."""
    try:
        html = driver.page_source.lower()
        if ('permanently closed' in html) or ('tutup permanen' in html):
            return 'Tutup Permanen'
        if ('temporarily closed' in html) or ('tutup sementara' in html):
            return 'Tutup Sementara'
        if any(w in html for w in ['open now', 'opens', 'closes', 'hours', 'jam', 'buka']):
            return 'Aktif'
        if ('closed' in html) or ('ditutup' in html):
            return 'Tutup'
        if ('open' in html) or ('buka' in html):
            return 'Aktif'
    except Exception:
        pass
    return 'Aktif'

# Fungsi untuk mendapatkan informasi dari Google Maps berdasarkan nama tempat
def get_place_info(driver, place_name, max_result=5, timeout=20):
    """Use an existing driver to search and scrape up to max_result items sequentially."""
    wait = WebDriverWait(driver, timeout)
    results_data = []
    search_url = f"https://www.google.com/maps/search/{quote(place_name)}"
    try:
        driver.get(search_url)
    except Exception as e:
        logging.warning(f"Gagal membuka URL untuk '{place_name}': {e}")
        return [{"Place": place_name, "Actual Place Name": "Error: Gagal membuka URL", "Address": "Gagal", "Phone Number": "Gagal", "Website": "Gagal", "Latitude": "Gagal", "Longitude": "Gagal", "Status": "Error"}]

    time.sleep(3)
    
    # Scroll panel hasil untuk memuat semua cards
    try:
        results_panel = driver.find_element(By.CSS_SELECTOR, "div[role='main']")
        for _ in range(3):  # Scroll 3x untuk memuat lebih banyak results
            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", results_panel)
            time.sleep(0.5)
    except Exception:
        pass
    
    # Coba scrape langsung dari card list (tanpa klik)
    # Selector yang lebih spesifik - hanya ambil card yang punya link
    card_link_selectors = [
        "a.hfpxzc"
    ]
    
    card_links = []
    for sel in card_link_selectors:
        try:
            card_links = driver.find_elements(By.CSS_SELECTOR, sel)
            if card_links:
                logging.info(f"Ditemukan {len(card_links)} valid place cards")
                break
        except Exception:
            continue
    
    # Scrape langsung dari list cards berdasarkan link (lebih akurat)
    if card_links and len(card_links) > 0:
        logging.info(f"Scraping langsung dari {min(max_result, len(card_links))} cards...")
        for i, link in enumerate(card_links[:max_result]):
            try:
                # Cari parent container yang tepat - div.Nv2PK.fontBodyMedium adalah container data
                # Gunakan ancestor::*[1] untuk mendapat parent terdekat yang match
                try:
                    card = link.find_element(By.XPATH, "./ancestor::div[contains(@class, 'Nv2PK')][1]")
                except:
                    # Fallback jika tidak ketemu
                    card = link.find_element(By.XPATH, "./ancestor::div[contains(@class, 'm6QErb')][1]")
                
                # Debugging: print class dari card untuk memastikan
                card_class = card.get_attribute("class")
                logging.debug(f"Card {i+1} class: {card_class}")
                
                data = {
                    "Place": place_name,
                    "Actual Place Name": "Gagal",
                    "Category": "Gagal",
                    "Rating": "Gagal",
                    "Address": "Gagal",
                    "Phone Number": "Gagal",
                    "Website": "Gagal",
                    "Latitude": "Gagal",
                    "Longitude": "Gagal",
                    "Status": "Aktif",
                    "Operation Hours": "Gagal",
                    "Open Status": "Gagal"
                }
                
                # === 1. NAMA TEMPAT - dari aria-label atau div.qBF1Pd ===
                name_found = False
                try:
                    aria_label = link.get_attribute("aria-label")
                    if aria_label and len(aria_label) > 0:
                        name = aria_label.split("·")[0].strip()
                        invalid_names = ['foto & video', 'foto', 'video', 'photos', 'reviews', 'menu', 'about', '']
                        if name.lower() not in invalid_names and len(name) > 2:
                            data["Actual Place Name"] = name
                            logging.info(f"Card {i+1}: ✓ Nama: {name[:50]}")
                            name_found = True
                except Exception as e:
                    logging.debug(f"Card {i+1}: Gagal dari aria-label: {str(e)[:30]}")
                
                if not name_found:
                    try:
                        elem = card.find_element(By.CSS_SELECTOR, ".qBF1Pd.fontHeadlineSmall")
                        name = elem.text.strip()
                        invalid_names = ['foto & video', 'foto', 'video', 'photos', 'reviews', 'menu', 'about', '']
                        if name and len(name) > 2 and name.lower() not in invalid_names:
                            data["Actual Place Name"] = name
                            logging.info(f"Card {i+1}: ✓ Nama (fallback): {name[:50]}")
                            name_found = True
                    except Exception:
                        pass
                
                if not name_found:
                    logging.warning(f"Card {i+1}: Gagal ambil nama, skip")
                    continue
                
                # === 2. CATEGORY - dari span, cari yang bukan rating/address/phone ===
                try:
                    # Cari semua span dalam card, filter yang merupakan category
                    all_category_spans = card.find_elements(By.XPATH, ".//span")
                    for elem in all_category_spans:
                        category_text = elem.text.strip()
                        if not category_text or len(category_text) < 3:
                            continue
                        # Skip jika ada angka (kemungkinan rating atau phone)
                        if any(c.isdigit() for c in category_text):
                            continue
                        # Skip jika ada keyword alamat
                        if any(kw in category_text.lower() for kw in ['jl', 'jalan', 'street', 'no.', 'blok', 'rt.', 'rw.']):
                            continue
                        # Skip jika terlalu panjang
                        if len(category_text) > 50:
                            continue
                        # Skip jika mengandung simbol phone/address
                        if any(char in category_text for char in ['+', '(', ')', '-', '/'] if category_text.count(char) > 1):
                            continue
                        # Ini kemungkinan category
                        data["Category"] = category_text
                        logging.info(f"Card {i+1}: ✓ Category: {category_text}")
                        break
                except Exception as e:
                    logging.debug(f"Card {i+1}: Category error: {str(e)[:50]}")
                    pass
                
                # === 3. RATING - dari span dengan aria-label atau MW4etd (dalam card ini) ===
                try:
                    rating_elem = card.find_element(By.XPATH, ".//span[contains(@class, 'MW4etd')]")
                    rating_text = rating_elem.text.strip()
                    if rating_text and len(rating_text) > 0:
                        data["Rating"] = rating_text
                        logging.info(f"Card {i+1}: ✓ Rating: {rating_text}")
                except Exception:
                    # Alternatif: cari dari aria-label yang mengandung rating
                    try:
                        rating_elem = card.find_element(By.XPATH, ".//span[contains(@aria-label, 'star') or contains(@aria-label, 'stars')]")
                        aria_rating = rating_elem.get_attribute("aria-label")
                        if aria_rating:
                            # Extract number dari "4.5 stars" atau similar
                            import re
                            match = re.search(r'(\d+\.?\d*)', aria_rating)
                            if match:
                                data["Rating"] = match.group(1)
                                logging.info(f"Card {i+1}: ✓ Rating (aria): {match.group(1)}")
                    except Exception:
                        # Jika tidak ada rating, set "No reviews"
                        try:
                            no_review = card.find_element(By.XPATH, ".//span[contains(text(), 'No reviews') or contains(text(), 'review')]")
                            if no_review:
                                data["Rating"] = "No reviews"
                                logging.info(f"Card {i+1}: ✓ Rating: No reviews")
                        except Exception:
                            pass
                
                # === 4 & 5. ALAMAT dan PHONE - dari semua span dalam card ===
                try:
                    # PENTING: Gunakan .// untuk mencari hanya dalam card ini
                    all_spans = card.find_elements(By.XPATH, ".//span")
                    
                    # Kumpulkan semua teks untuk debugging
                    span_texts = [s.text.strip() for s in all_spans if s.text.strip()]
                    logging.debug(f"Card {i+1}: Found {len(span_texts)} spans with text")
                    
                    for elem in all_spans:
                        text = elem.text.strip()
                        if not text or len(text) < 8:
                            continue
                        
                        # Skip jika sudah dapat keduanya
                        if data["Address"] != "Gagal" and data["Phone Number"] != "Gagal":
                            break
                        
                        # Cek apakah ini alamat (prioritas lebih tinggi)
                        if data["Address"] == "Gagal":
                            address_keywords = ['jl.', 'jl ', 'jalan', 'street', 'no.', 'no ', 'blok', 'rt.', 'rw.', 'kec.', 'kel.']
                            if any(kw in text.lower() for kw in address_keywords):
                                # Pastikan bukan pure number
                                if not text.replace('-', '').replace('.', '').replace(' ', '').replace('/', '').isdigit():
                                    # Pastikan tidak dimulai dengan karakter phone
                                    if len(text) > 0 and text[0] not in ['+', '0', '(']:
                                        data["Address"] = text
                                        logging.info(f"Card {i+1}: ✓ Alamat: {text[:50]}")
                                        continue
                        
                        # Cek apakah ini phone number
                        if data["Phone Number"] == "Gagal":
                            # Phone harus dimulai dengan karakter phone
                            if len(text) > 0 and text[0] in ['+', '0', '(', '6', '8']:
                                # Hitung jumlah digit
                                digit_count = sum(c.isdigit() for c in text)
                                if digit_count >= 6:
                                    # Tidak boleh ada keyword alamat
                                    invalid_keywords = ['jl.', 'jl ', 'jalan', 'street', 'blok', 'rt.', 'rw.', 'kec.', 'kel.']
                                    if not any(kw in text.lower() for kw in invalid_keywords):
                                        data["Phone Number"] = text
                                        logging.info(f"Card {i+1}: ✓ Phone: {text}")
                                        continue
                except Exception as e:
                    logging.debug(f"Card {i+1}: Address/Phone error: {str(e)[:50]}")
                    pass
                
                # === 6. OPERATION HOURS & STATUS - dari span dalam card ===
                try:
                    # Cari semua span yang mengandung info jam
                    hours_spans = card.find_elements(By.XPATH, ".//span")
                    
                    full_hours_text = []
                    for elem in hours_spans:
                        text = elem.text.strip()
                        if text and any(kw in text.lower() for kw in ['open', 'close', 'buka', 'tutup', 'am', 'pm', 'wib']):
                            full_hours_text.append(text)
                            
                            # Tentukan status berdasarkan keyword
                            if any(kw in text.lower() for kw in ['permanently closed', 'tutup permanen', 'closed permanently']):
                                data["Status"] = "Tutup Permanen"
                                data["Open Status"] = text
                            elif any(kw in text.lower() for kw in ['temporarily closed', 'tutup sementara', 'closed temporarily']):
                                data["Status"] = "Tutup Sementara"
                                data["Open Status"] = text
                            elif 'closed' in text.lower() or 'tutup' in text.lower():
                                if 'open' not in text.lower():
                                    data["Status"] = "Tutup"
                                    data["Open Status"] = text
                            elif 'open' in text.lower() or 'buka' in text.lower():
                                data["Status"] = "Aktif"
                                if not data["Open Status"] or data["Open Status"] == "Gagal":
                                    data["Open Status"] = text
                    
                    # Gabungkan semua teks jam jika ada
                    if full_hours_text:
                        # Filter yang benar-benar jam (ada angka)
                        hours_with_time = [h for h in full_hours_text if any(c.isdigit() for c in h)]
                        if hours_with_time:
                            data["Operation Hours"] = " · ".join(hours_with_time[:2])  # Ambil max 2 elemen
                            logging.info(f"Card {i+1}: ✓ Hours: {data['Operation Hours']}")
                        if data["Open Status"] != "Gagal":
                            logging.info(f"Card {i+1}: ✓ Open Status: {data['Open Status']}")
                except Exception as e:
                    logging.debug(f"Card {i+1}: Hours/Status error: {str(e)[:50]}")
                    pass
                
                # === 7. WEBSITE - coba dari aria-label atau attribute ===
                # Google Maps cards biasanya tidak menampilkan website di list, hanya di detail
                # Tapi kita tetap coba
                try:
                    website_elem = card.find_element(By.CSS_SELECTOR, "a[data-value='website']")
                    website_url = website_elem.get_attribute("href")
                    if website_url:
                        data["Website"] = website_url
                        logging.info(f"Card {i+1}: ✓ Website: {website_url[:50]}")
                except Exception:
                    pass
                
                # === 8. KOORDINAT - dari href link ===
                # Get URL untuk extract koordinat dari link yang sudah ada
                try:
                    url = link.get_attribute("href")
                    if url:
                        lat, lon = extract_coords_from_url(url)
                        if lat and lon:
                            data["Latitude"], data["Longitude"] = lat, lon
                            logging.info(f"Card {i+1}: ✓ Koordinat: {lat}, {lon}")
                except Exception:
                    pass
                
                results_data.append(data)
                
            except Exception as e:
                logging.warning(f"Card {i+1}: Error - {str(e)[:100]}")
                continue
        
        if results_data:
            logging.info(f"✓ Berhasil scrape {len(results_data)} cards dari list")
            return results_data
    
    # Fallback: coba metode lama (klik satu-satu)
    logging.info("Fallback ke metode klik card satu-satu...")
    cards_selector_candidates = [".hfpxzc", ".Nv2PK"]
    cards = []
    for sel in cards_selector_candidates:
        try:
            cards = driver.find_elements(By.CSS_SELECTOR, sel)
            if cards:
                break
        except Exception:
            continue

    # Jika tidak ada kartu, mungkin langsung ke halaman tempat atau tidak ditemukan
    if not cards:
        # Wait for place details panel to load
        try:
            WebDriverWait(driver, 8).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "h1.DUwDvf"))
            )
        except Exception as e:
            # Tidak ada hasil atau halaman tidak muncul
            logging.warning(f"Tidak ditemukan hasil untuk '{place_name}': {str(e)[:100]}")
            return [{"Place": place_name, "Actual Place Name": "Tidak ditemukan - No results", "Address": "Gagal", "Phone Number": "Gagal", "Website": "Gagal", "Latitude": "Gagal", "Longitude": "Gagal", "Status": "Error"}]
        
        time.sleep(1)
        
        data = {
            "Place": place_name,
            "Actual Place Name": "Gagal",
            "Category": "Gagal",
            "Rating": "Gagal",
            "Address": "Gagal",
            "Phone Number": "Gagal",
            "Website": "Gagal",
            "Latitude": "Gagal",
            "Longitude": "Gagal",
            "Status": get_place_status(driver),
            "Operation Hours": "Gagal",
            "Open Status": "Gagal"
        }
        try:
            actual_name = driver.find_element(By.CSS_SELECTOR, "h1.DUwDvf").text
            if actual_name and len(actual_name.strip()) > 0:
                data["Actual Place Name"] = actual_name
            else:
                logging.warning(f"Place name kosong untuk '{place_name}'")
                return [{"Place": place_name, "Actual Place Name": "Error: Nama tempat kosong", "Address": "Gagal", "Phone Number": "Gagal", "Website": "Gagal", "Latitude": "Gagal", "Longitude": "Gagal", "Status": "Error"}]
        except Exception as e:
            logging.error(f"Gagal mendapatkan place name untuk '{place_name}': {str(e)[:100]}")
            return [{"Place": place_name, "Actual Place Name": f"Error: {str(e)[:50]}", "Address": "Gagal", "Phone Number": "Gagal", "Website": "Gagal", "Latitude": "Gagal", "Longitude": "Gagal", "Status": "Error"}]
        try:
            data["Address"] = driver.find_element(By.CSS_SELECTOR, '[data-item-id="address"]').text
        except Exception:
            pass
        try:
            phone_el = driver.find_element(By.CSS_SELECTOR, '[data-item-id="phone"]')
            data["Phone Number"] = phone_el.text
        except Exception:
            pass
        try:
            we = driver.find_element(By.CSS_SELECTOR, '[data-item-id="authority"]')
            data["Website"] = we.get_attribute("href")
        except Exception:
            pass
        
        # Rating dari detail page
        try:
            rating_elem = driver.find_element(By.CSS_SELECTOR, "div.F7nice > span > span[aria-hidden='true']")
            rating_text = rating_elem.text.strip()
            if rating_text:
                data["Rating"] = rating_text
        except Exception:
            try:
                rating_elem = driver.find_element(By.CSS_SELECTOR, "span.ceNzKf[aria-label*='star' i]")
                aria_rating = rating_elem.get_attribute("aria-label")
                if aria_rating:
                    match = re.search(r'(\d+\.?\d*)', aria_rating)
                    if match:
                        data["Rating"] = match.group(1)
            except Exception:
                pass
        
        # Try multiple selectors untuk Category
        category_selectors = [
            (By.CSS_SELECTOR, "button.DkEaL"),
            (By.XPATH, "//button[contains(@class, 'DkEaL')]"),
            (By.CSS_SELECTOR, "button[jsaction*='category']"),
            (By.XPATH, "//div[@class='fontBodyMedium dmRWX']//button")
        ]
        
        for by, selector in category_selectors:
            try:
                elem = driver.find_element(by, selector)
                type_text = elem.text
                if type_text and len(type_text.strip()) > 0:
                    data["Category"] = type_text.strip()
                    break
            except Exception:
                continue
        
        # Try multiple selectors untuk Open Status
        status_selectors = [
            (By.XPATH, "//span[contains(@class, 'ZDu9vd')]//span[2]"),
            (By.CSS_SELECTOR, "span.ZDu9vd span:nth-child(2)"),
            (By.XPATH, "//div[contains(@aria-label, 'Hours')]//span[contains(text(), 'Open') or contains(text(), 'Closed') or contains(text(), 'Buka') or contains(text(), 'Tutup')]"),
            (By.XPATH, "//div[contains(text(), 'Opens') or contains(text(), 'Closes') or contains(text(), 'Buka') or contains(text(), 'Tutup')]")
        ]
        
        for by, selector in status_selectors:
            try:
                elem = driver.find_element(by, selector)
                status_text = elem.text
                if status_text and len(status_text.strip()) > 0:
                    data["Open Status"] = status_text.strip()
                    
                    # Update status based on open status text
                    if any(kw in status_text.lower() for kw in ['permanently closed', 'tutup permanen']):
                        data["Status"] = "Tutup Permanen"
                    elif any(kw in status_text.lower() for kw in ['temporarily closed', 'tutup sementara']):
                        data["Status"] = "Tutup Sementara"
                    elif 'closed' in status_text.lower() or 'tutup' in status_text.lower():
                        data["Status"] = "Tutup"
                    else:
                        data["Status"] = "Aktif"
                    break
            except Exception:
                continue
        
        # Extract Operation Hours dari tombol/div hours
        try:
            hours_button = driver.find_element(By.CSS_SELECTOR, "button[data-item-id='oh']")
            hours_aria = hours_button.get_attribute("aria-label")
            if hours_aria:
                data["Operation Hours"] = hours_aria
        except Exception:
            try:
                # Alternatif: cari dari div yang menampilkan jam
                hours_div = driver.find_element(By.XPATH, "//div[contains(@class, 'ZDu9vd')]//span")
                hours_text = hours_div.text.strip()
                if hours_text and re.search(r'\d{1,2}[:.\-]\d{2}', hours_text):
                    data["Operation Hours"] = hours_text
            except Exception:
                pass
        
        lat, lon = wait_for_coords_in_url(driver, max_wait=10)
        if not lat or not lon:
            lat, lon = extract_coords_from_page(driver)
        if lat and lon:
            data["Latitude"], data["Longitude"] = lat, lon
        data["Status"] = get_place_status(driver)
        results_data.append(data)
        return results_data

    # Jika ada list hasil, klik satu per satu
    for i in range(min(max_result, len(cards))):
        try:
            cards = cards if i < len(cards) else driver.find_elements(By.CSS_SELECTOR, cards_selector_candidates[0])
            if i >= len(cards):
                break
            driver.execute_script("arguments[0].scrollIntoView(true);", cards[i])
            time.sleep(0.8)
            old_url = driver.current_url
            driver.execute_script("arguments[0].click();", cards[i])
            
            # Wait for place details panel to load dengan multiple attempts
            detail_loaded = False
            for attempt in range(3):
                try:
                    WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "h1.DUwDvf"))
                    )
                    detail_loaded = True
                    logging.info(f"Card {i+1}: Detail panel loaded")
                    break
                except Exception as e:
                    logging.warning(f"Card {i+1}: Attempt {attempt+1} - Detail panel not loaded: {str(e)[:50]}")
                    time.sleep(1)
            
            if not detail_loaded:
                logging.error(f"Card {i+1}: Detail panel gagal load setelah 3 attempts")
                continue
            
            # Extra wait untuk memastikan semua element loaded
            time.sleep(1.5)
            
            # Wait for URL to update with coordinates
            lat, lon = wait_for_coords_in_url(driver, max_wait=12)

            data = {
                "Place": place_name,
                "Actual Place Name": "Gagal",
                "Category": "Gagal",
                "Rating": "Gagal",
                "Address": "Gagal",
                "Phone Number": "Gagal",
                "Website": "Gagal",
                "Latitude": "Gagal",
                "Longitude": "Gagal",
                "Status": "Aktif",
                "Operation Hours": "Gagal",
                "Open Status": "Gagal"
            }
            # Try multiple selectors untuk actual place name
            actual_name = None
            name_selectors = [
                (By.CSS_SELECTOR, "h1.DUwDvf"),
                (By.CSS_SELECTOR, "h1.fontHeadlineLarge"),
                (By.XPATH, "//h1[@class='DUwDvf lfPIob']"),
                (By.XPATH, "/html/body/div[1]/div[2]/div[9]/div[8]/div/div/div[1]/div[2]/div/div[1]/div/div/div[1]/div[1]/div[1]/div/div[2]/div[4]/div[1]/div/div/div[2]/div[1]/div[2]"),
                (By.XPATH, "//div[@class='fontHeadlineLarge']//span"),
                (By.CSS_SELECTOR, "div.fontHeadlineLarge span")
            ]
            
            for by, selector in name_selectors:
                try:
                    # Wait untuk element muncul dulu
                    WebDriverWait(driver, 3).until(
                        EC.presence_of_element_located((by, selector))
                    )
                    elem = driver.find_element(by, selector)
                    actual_name = elem.text
                    if actual_name and len(actual_name.strip()) > 0:
                        data["Actual Place Name"] = actual_name.strip()
                        logging.info(f"Card {i+1}: ✓ Nama: {actual_name.strip()[:50]} (via {by})")
                        break
                except Exception as e:
                    logging.debug(f"Card {i+1}: Selector {by} gagal: {str(e)[:30]}")
                    continue
            
            if not actual_name or len(actual_name.strip()) == 0:
                logging.error(f"Card {i+1}: ✗ Semua selector nama gagal untuk '{place_name}'")
                data["Actual Place Name"] = f"Error: Gagal ambil nama (card {i+1})"
                continue
            # Try multiple selectors untuk address
            address_selectors = [
                (By.CSS_SELECTOR, '[data-item-id="address"]'),
                (By.XPATH, "//button[@data-item-id='address']//div[contains(@class, 'fontBodyMedium')]"),
                (By.CSS_SELECTOR, "button[data-item-id='address'] div.fontBodyMedium"),
                (By.XPATH, "//div[@class='Io6YTe fontBodyMedium kR99db fdkmkc']")
            ]
            
            for by, selector in address_selectors:
                try:
                    elem = driver.find_element(by, selector)
                    address_text = elem.text
                    if address_text and len(address_text.strip()) > 0:
                        data["Address"] = address_text.strip()
                        logging.info(f"Card {i+1}: ✓ Alamat ditemukan")
                        break
                except Exception:
                    continue
            
            # Phone Number
            try:
                phone_el = driver.find_element(By.CSS_SELECTOR, '[data-item-id="phone"]')
                phone_text = phone_el.text
                if phone_text and len(phone_text.strip()) > 0:
                    data["Phone Number"] = phone_text.strip()
                    logging.info(f"Card {i+1}: ✓ Phone ditemukan")
            except Exception:
                logging.debug(f"Card {i+1}: Phone tidak ditemukan")
                pass
            
            # Website
            try:
                we = driver.find_element(By.CSS_SELECTOR, '[data-item-id="authority"]')
                website_url = we.get_attribute("href")
                if website_url:
                    data["Website"] = website_url
                    logging.info(f"Card {i+1}: ✓ Website ditemukan")
            except Exception:
                logging.debug(f"Card {i+1}: Website tidak ditemukan")
                pass
            
            # Rating dari detail page
            try:
                rating_elem = driver.find_element(By.CSS_SELECTOR, "div.F7nice > span > span[aria-hidden='true']")
                rating_text = rating_elem.text.strip()
                if rating_text:
                    data["Rating"] = rating_text
                    logging.info(f"Card {i+1}: ✓ Rating: {rating_text}")
            except Exception:
                try:
                    rating_elem = driver.find_element(By.CSS_SELECTOR, "span.ceNzKf[aria-label*='star' i]")
                    aria_rating = rating_elem.get_attribute("aria-label")
                    if aria_rating:
                        match = re.search(r'(\d+\.?\d*)', aria_rating)
                        if match:
                            data["Rating"] = match.group(1)
                            logging.info(f"Card {i+1}: ✓ Rating: {match.group(1)}")
                except Exception:
                    pass
            
            # Try multiple selectors untuk Category
            category_selectors = [
                (By.CSS_SELECTOR, "button.DkEaL"),
                (By.XPATH, "//button[contains(@class, 'DkEaL')]"),
                (By.CSS_SELECTOR, "button[jsaction*='category']"),
                (By.XPATH, "//div[@class='fontBodyMedium dmRWX']//button")
            ]
            
            for by, selector in category_selectors:
                try:
                    elem = driver.find_element(by, selector)
                    category_text = elem.text
                    if category_text and len(category_text.strip()) > 0:
                        data["Category"] = category_text.strip()
                        logging.info(f"Card {i+1}: ✓ Category: {category_text.strip()}")
                        break
                except Exception:
                    continue
            
            # Try multiple selectors untuk Open Status
            status_selectors = [
                (By.XPATH, "//span[contains(@class, 'ZDu9vd')]//span[2]"),
                (By.CSS_SELECTOR, "span.ZDu9vd span:nth-child(2)"),
                (By.XPATH, "//div[contains(@aria-label, 'Hours')]//span[contains(text(), 'Open') or contains(text(), 'Closed') or contains(text(), 'Buka') or contains(text(), 'Tutup')]"),
                (By.XPATH, "//div[contains(text(), 'Opens') or contains(text(), 'Closes') or contains(text(), 'Buka') or contains(text(), 'Tutup')]")
            ]
            
            for by, selector in status_selectors:
                try:
                    elem = driver.find_element(by, selector)
                    status_text = elem.text
                    if status_text and len(status_text.strip()) > 0:
                        data["Open Status"] = status_text.strip()
                        logging.info(f"Card {i+1}: ✓ Open Status: {status_text.strip()}")
                        
                        # Update status based on text
                        if any(kw in status_text.lower() for kw in ['permanently closed', 'tutup permanen']):
                            data["Status"] = "Tutup Permanen"
                        elif any(kw in status_text.lower() for kw in ['temporarily closed', 'tutup sementara']):
                            data["Status"] = "Tutup Sementara"
                        elif 'closed' in status_text.lower() or 'tutup' in status_text.lower():
                            data["Status"] = "Tutup"
                        else:
                            data["Status"] = "Aktif"
                        break
                except Exception:
                    continue
            
            # Extract Operation Hours
            try:
                hours_button = driver.find_element(By.CSS_SELECTOR, "button[data-item-id='oh']")
                hours_aria = hours_button.get_attribute("aria-label")
                if hours_aria:
                    data["Operation Hours"] = hours_aria
                    logging.info(f"Card {i+1}: ✓ Hours: {hours_aria[:50]}...")
            except Exception:
                try:
                    hours_div = driver.find_element(By.XPATH, "//div[contains(@class, 'ZDu9vd')]//span")
                    hours_text = hours_div.text.strip()
                    if hours_text and re.search(r'\d{1,2}[:.\-]\d{2}', hours_text):
                        data["Operation Hours"] = hours_text
                        logging.info(f"Card {i+1}: ✓ Hours: {hours_text}")
                except Exception:
                    pass
            
            # Get coordinates
            if not lat or not lon:
                lat, lon = extract_coords_from_page(driver)
            if lat and lon:
                data["Latitude"], data["Longitude"] = lat, lon
                logging.info(f"Card {i+1}: ✓ Koordinat: {lat}, {lon}")
            else:
                logging.warning(f"Card {i+1}: ✗ Koordinat tidak ditemukan")
                
            data["Status"] = get_place_status(driver)
            
            # Validasi data sebelum menambahkan ke hasil
            if data["Actual Place Name"] != "Gagal" and not data["Actual Place Name"].startswith("Error:"):
                results_data.append(data)
                logging.info(f"✓ Berhasil scrape card {i+1}: {data['Actual Place Name']}")
            else:
                logging.warning(f"✗ Gagal scrape card {i+1}: {data['Actual Place Name']}")
                
        except Exception as e:
            logging.warning(f"Error di card {i+1} untuk '{place_name}': {str(e)[:100]}")
            continue

    # Jika tidak ada hasil yang valid, return error message
    if not results_data:
        logging.error(f"Semua card gagal untuk '{place_name}'. Kemungkinan multiple results atau elemen tidak ditemukan.")
        return [{"Place": place_name, "Actual Place Name": "Error: Multiple results - gagal semua kartu", "Address": "Gagal", "Phone Number": "Gagal", "Website": "Gagal", "Latitude": "Gagal", "Longitude": "Gagal", "Status": "Error"}]
    
    return results_data

def save_batch_results(results, output_csv, append_mode=False):
    """
    Simpan hasil ke CSV dengan opsi append atau replace.
    Jika append_mode=True dan file sudah ada, akan di-append.
    """
    if not results:
        return
    
    df_result = pd.DataFrame(results)
    
    # Clean text
    def clean_text(x):
        if isinstance(x, str):
            return (
                x.replace('', '')
                .replace('\n', ' ')
                .replace('\r', ' ')
                .strip()
            )
        return x
    
    df_result = df_result.map(clean_text)
    
    # Tentukan kolom utama dan tambahan
    kolom_utama = [
        'idsbr', 'Query', 'Actual Place Name', 'Category', 'Rating',
        'Address', 'Phone Number', 'Website', 'Latitude', 'Longitude',
        'Status', 'Open Status', 'Operation Hours'
    ]
    kolom_utama = [col for col in kolom_utama if col in df_result.columns]
    kolom_lain = [c for c in df_result.columns if c not in kolom_utama]
    df_result = df_result[kolom_utama + kolom_lain]
    
    # Jika append mode dan file sudah ada, baca dan append
    if append_mode and os.path.exists(output_csv):
        try:
            df_existing = pd.read_csv(output_csv)
            df_result = pd.concat([df_existing, df_result], ignore_index=True)
            print(f"  📊 Append {len(results)} items ke {len(df_existing)} existing rows")
        except Exception as e:
            print(f"  ⚠️  Tidak bisa append: {e}. Save sebagai baru.")
    
    df_result.to_csv(output_csv, index=False)
    print(f"  💾 Saved ke: {output_csv}")


def load_existing_results(output_csv):
    """Load hasil yang sudah ada untuk resume."""
    if os.path.exists(output_csv):
        try:
            df = pd.read_csv(output_csv)
            existing_ids = set(df['idsbr'].astype(str).values)
            print(f"📂 Ditemukan {len(existing_ids)} hasil sebelumnya. Akan skip yang sudah ada.")
            return existing_ids
        except Exception as e:
            print(f"⚠️  Tidak bisa load existing: {e}")
    return set()

     
def process_single_query(args):
    """
    Worker function untuk parallel processing.
    Setiap worker membuat driver sendiri, scrape satu query, lalu tutup driver.
    """
    idsbr, query, worker_id = args
    driver = None
    results_list = []
    
    try:
        driver = init_driver(headless=True, timeout=25)
        print(f"  [Worker {worker_id}] 🔍 {idsbr} | {query}")
        
        result = get_place_info(driver, query, max_result=5, timeout=25)
        
        if isinstance(result, list):
            for r in result:
                r['idsbr'] = idsbr
                r['Query'] = query
                # Cek apakah ada error
                if r.get('Actual Place Name', '').startswith('Error:') or r.get('Actual Place Name') == 'Gagal':
                    r['Status'] = 'Error'
                    print(f"  [Worker {worker_id}] ⚠️  Gagal: {r['Actual Place Name']}")
                else:
                    if 'Status' not in r or not r['Status']:
                        r['Status'] = 'Aktif'
                    print(f"  [Worker {worker_id}] ✅ Berhasil: {r.get('Actual Place Name', 'N/A')}")
                results_list.append(r)
        else:
            result['idsbr'] = idsbr
            result['Query'] = query
            # Cek apakah ada error
            if result.get('Actual Place Name', '').startswith('Error:') or result.get('Actual Place Name') == 'Gagal':
                result['Status'] = 'Error'
                print(f"  [Worker {worker_id}] ⚠️  Gagal: {result['Actual Place Name']}")
            else:
                if 'Status' not in result or not result['Status']:
                    result['Status'] = 'Aktif'
                print(f"  [Worker {worker_id}] ✅ Berhasil: {result.get('Actual Place Name', 'N/A')}")
            results_list.append(result)
            
    except Exception as e:
        print(f"  [Worker {worker_id}] ❌ Error: {str(e)[:100]}")
        results_list.append({
            'idsbr': idsbr,
            'Query': query,
            'Actual Place Name': 'Gagal',
            'Status': 'Error'
        })
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
    
    return results_list


def scraping_csv_idsbr_ke_csv(
    input_csv="/home/vanaprastha/Videos/PRODUK/MAGANG /Program/Scrapping/carimap.csv",
    output_csv="/home/vanaprastha/Videos/PRODUK/MAGANG /Program/Scrapping/carimap2.csv",
    max_workers=10  # Jumlah browser parallel
):
    # Mulai timer
    start_time = time.time()
    
    print(f"Membaca file: {input_csv}")

    df = pd.read_csv(
        input_csv,
        header=None,
        encoding="ISO-8859-1",
        usecols=[0, 1],
        engine="python"
    )
    df.columns = ['idsbr', 'query']
    df['idsbr'] = (
        df['idsbr']
        .astype(str)
        .str.replace('ï»¿', '', regex=False)
        .str.strip()
    )
    df['query'] = df['query'].astype(str).str.strip()
    df = df[df['query'] != '']

    print(f"Total query valid: {len(df)}")
    
    # Load existing results untuk resume
    existing_ids = load_existing_results(output_csv)
    df_to_scrape = df[~df['idsbr'].astype(str).isin(existing_ids)].reset_index(drop=True)
    
    if len(df_to_scrape) == 0:
        print("✅ Semua data sudah di-scrape sebelumnya!")
        return

    print(f"⏳ Akan scrape {len(df_to_scrape)} query baru dengan {max_workers} workers parallel")

    # ================================
    # SCRAPING PARALLEL - ThreadPoolExecutor
    # ================================
    results = []
    completed = 0
    total = len(df_to_scrape)
    
    # Siapkan arguments untuk setiap query
    # Format: (idsbr, query, worker_id)
    query_args = [
        (row['idsbr'], row['query'], idx % max_workers + 1) 
        for idx, (_, row) in enumerate(df_to_scrape.iterrows())
    ]
    
    # Process menggunakan ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_query = {
            executor.submit(process_single_query, args): args 
            for args in query_args
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_query):
            args = future_to_query[future]
            completed += 1
            
            try:
                result_list = future.result()
                results.extend(result_list)
                print(f"\n[{completed}/{total}] Completed: {args[0]} | {args[1][:50]}...")
            except Exception as e:
                print(f"\n[{completed}/{total}] ❌ Future error for {args[0]}: {str(e)[:100]}")
                results.append({
                    'idsbr': args[0],
                    'Query': args[1],
                    'Actual Place Name': 'Gagal',
                    'Status': 'Error'
                })
            
            # Save batch setiap 50 item
            if completed % 50 == 0:
                print(f"\n📋 Saving batch at {completed}/{total}...")
                save_batch_results(results, output_csv, append_mode=(completed > 50))
                results = []
            
            # Small delay untuk menghindari rate limiting
            time.sleep(0.5)
    
    # Save hasil akhir jika ada sisa
    if results:
        print(f"\n📋 Saving final batch ({len(results)} items)...")
        save_batch_results(results, output_csv, append_mode=True)

    # Hitung durasi eksekusi
    end_time = time.time()
    elapsed_seconds = end_time - start_time
    hours = int(elapsed_seconds // 3600)
    minutes = int((elapsed_seconds % 3600) // 60)
    seconds = int(elapsed_seconds % 60)
    
    print("\n" + "="*50)
    print("✅ SCRAPING SELESAI")
    print(f"📁 File tersimpan di: {output_csv}")
    print(f"📊 Total query di-scrape: {total}")
    print(f"⏱️  Waktu eksekusi: {hours} jam {minutes} menit {seconds} detik")
    if total > 0:
        avg_per_query = elapsed_seconds / total
        print(f"📈 Rata-rata per query: {avg_per_query:.2f} detik")
    print("="*50)

if __name__ == "__main__":
    scraping_csv_idsbr_ke_csv()
