# =====================================================
# CEQA Warehouse Projects Scraper
# For Build Your City (BYC) - Inland Empire Mapping
# =====================================================

import os
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import re
from dataclasses import dataclass

# Web scraping
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from bs4 import BeautifulSoup

# Data processing
import pandas as pd
import requests
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut

# Database
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class CEQAProject:
    """Data structure for a scraped CEQA project"""
    title: str
    lead_agency: str
    city: str
    county: str
    address: str
    project_description: str
    project_type: str
    document_type: str
    ceqa_status: str
    date_posted: Optional[datetime]
    comment_deadline: Optional[datetime]
    ceqa_url: str
    document_urls: List[str]
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    is_warehouse: bool = False
    warehouse_confidence: float = 0.0
    detection_keywords: List[str] = None

class CEQAScraper:
    """Main scraper class for CEQA warehouse projects"""
    
    def __init__(self):
        # Supabase connection
        supabase_url = os.getenv('SUPABASE_URL')
        supabase_key = os.getenv('SUPABASE_ANON_KEY')
        
        if not supabase_url or not supabase_key:
            raise ValueError("Missing SUPABASE_URL or SUPABASE_ANON_KEY environment variables")
        
        self.supabase: Client = create_client(supabase_url, supabase_key)
        
        # Selenium setup
        self.driver = None
        self.wait = None
        
        # Geocoding
        self.geocoder = Nominatim(user_agent="byc_warehouse_scraper")
        
        # Warehouse detection keywords
        self.warehouse_keywords = {
            'high_confidence': ['warehouse', 'fulfillment', 'distribution center', 'logistics center'],
            'medium_confidence': ['industrial', 'cargo', 'freight', 'supply chain', 'e-commerce'],
            'low_confidence': ['storage', 'shipping', 'receiving', 'inventory']
        }
        
        # IE Cities for validation
        self.ie_cities = {
            'san bernardino': ['fontana', 'ontario', 'san bernardino', 'rialto', 'colton', 
                             'rancho cucamonga', 'upland', 'chino', 'montclair'],
            'riverside': ['riverside', 'moreno valley', 'perris', 'corona', 'norco', 
                         'eastvale', 'jurupa valley', 'lake elsinore', 'menifee']
        }

    def setup_driver(self, headless: bool = True):
        """Initialize Selenium WebDriver with proper configuration"""
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        
        # User agent to avoid detection
        chrome_options.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
        
        self.driver = webdriver.Chrome(options=chrome_options)
        self.wait = WebDriverWait(self.driver, 10)
        logger.info("WebDriver initialized")

    def navigate_to_ceqa_search(self):
        """Navigate to CEQA advanced search page"""
        try:
            self.driver.get("https://ceqanet.lci.ca.gov/Search/Advanced")
            logger.info("Navigated to CEQA advanced search")
            
            # Wait for page to load
            self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "form")))
            time.sleep(2)  # Additional buffer
            
        except TimeoutException:
            logger.error("Failed to load CEQA search page")
            raise

    def configure_search_filters(self):
        """Set up search filters for Inland Empire warehouse projects"""
        try:
            # Select counties: San Bernardino and Riverside
            county_select = Select(self.driver.find_element(By.ID, "CountyIds"))
            county_select.select_by_visible_text("San Bernardino")
            county_select.select_by_visible_text("Riverside")
            logger.info("Selected San Bernardino and Riverside counties")
            
            # Select project types: Industrial and Commercial
            project_type_select = Select(self.driver.find_element(By.ID, "ProjectTypeIds"))
            project_type_select.select_by_visible_text("Industrial")
            project_type_select.select_by_visible_text("Commercial")
            logger.info("Selected Industrial and Commercial project types")
            
            # Select document types for different project stages
            doc_type_select = Select(self.driver.find_element(By.ID, "DocumentTypeIds"))
            doc_types = ["NOP", "IS/MND", "EIR", "NOD"]
            for doc_type in doc_types:
                try:
                    doc_type_select.select_by_visible_text(doc_type)
                except NoSuchElementException:
                    logger.warning(f"Document type '{doc_type}' not found")
            
            # Set date range: 2020-2025
            start_date = self.driver.find_element(By.ID, "StartDate")
            end_date = self.driver.find_element(By.ID, "EndDate")
            start_date.clear()
            start_date.send_keys("01/01/2020")
            end_date.clear()
            end_date.send_keys("12/31/2025")
            logger.info("Set date range: 2020-2025")
            
            time.sleep(1)  # Let filters settle
            
        except Exception as e:
            logger.error(f"Error configuring search filters: {e}")
            raise

    def submit_search(self):
        """Submit the search form and wait for results"""
        try:
            search_button = self.driver.find_element(By.CSS_SELECTOR, "input[type='submit']")
            search_button.click()
            logger.info("Submitted search form")
            
            # Wait for results to load
            self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "search-results")))
            time.sleep(3)  # Additional buffer for content loading
            
        except TimeoutException:
            logger.error("Search results failed to load")
            raise

    def extract_project_links(self) -> List[str]:
        """Extract all project detail page URLs from search results"""
        project_links = []
        page_num = 1
        
        while True:
            try:
                # Get current page's project links
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                
                # Find project title links (adjust selector based on actual HTML)
                title_links = soup.find_all('a', href=re.compile(r'/Project/'))
                page_links = [f"https://ceqanet.lci.ca.gov{link['href']}" 
                             for link in title_links]
                
                project_links.extend(page_links)
                logger.info(f"Page {page_num}: Found {len(page_links)} projects")
                
                # Try to navigate to next page
                try:
                    next_button = self.driver.find_element(By.LINK_TEXT, "Next")
                    if "disabled" in next_button.get_attribute("class"):
                        break
                    next_button.click()
                    time.sleep(3)
                    page_num += 1
                except NoSuchElementException:
                    break
                    
            except Exception as e:
                logger.error(f"Error extracting project links on page {page_num}: {e}")
                break
        
        logger.info(f"Total projects found: {len(project_links)}")
        return project_links

    def scrape_project_details(self, project_url: str) -> Optional[CEQAProject]:
        """Scrape detailed information from a single project page"""
        try:
            self.driver.get(project_url)
            self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Extract basic information (adjust selectors based on actual HTML)
            title = self._extract_field(soup, "Project Title", "h1")
            lead_agency = self._extract_field(soup, "Lead Agency")
            location = self._extract_field(soup, "Location")
            description = self._extract_field(soup, "Project Description", "div")
            project_type = self._extract_field(soup, "Project Type")
            document_type = self._extract_field(soup, "Document Type")
            ceqa_status = self._extract_field(soup, "CEQA Status")
            
            # Parse location into city/county
            city, county = self._parse_location(location)
            
            # Extract dates
            date_posted = self._extract_date(soup, "Date Posted")
            comment_deadline = self._extract_date(soup, "Comment Deadline")
            
            # Extract document URLs
            document_urls = self._extract_document_urls(soup)
            
            # Create project object
            project = CEQAProject(
                title=title or "Unknown Project",
                lead_agency=lead_agency or "",
                city=city,
                county=county,
                address=location or "",
                project_description=description or "",
                project_type=project_type or "",
                document_type=document_type or "",
                ceqa_status=ceqa_status or "",
                date_posted=date_posted,
                comment_deadline=comment_deadline,
                ceqa_url=project_url,
                document_urls=document_urls
            )
            
            # Classify as warehouse and get geocoding
            self._classify_warehouse(project)
            self._geocode_project(project)
            
            return project
            
        except Exception as e:
            logger.error(f"Error scraping project {project_url}: {e}")
            return None

    def _extract_field(self, soup, field_name: str, tag: str = None) -> Optional[str]:
        """Extract a field value from the soup"""
        try:
            # Try multiple selector strategies
            selectors = [
                f"*:contains('{field_name}') + *",
                f"*:contains('{field_name}')",
                f"dt:contains('{field_name}') + dd",
                f"label:contains('{field_name}') + *"
            ]
            
            for selector in selectors:
                elements = soup.select(selector)
                if elements:
                    return elements[0].get_text(strip=True)
            
            # Fallback: find by text content
            for elem in soup.find_all(text=re.compile(field_name, re.I)):
                parent = elem.parent
                if parent and parent.next_sibling:
                    return parent.next_sibling.get_text(strip=True)
            
            return None
            
        except Exception as e:
            logger.debug(f"Could not extract field '{field_name}': {e}")
            return None

    def _parse_location(self, location: str) -> Tuple[str, str]:
        """Parse location string into city and county"""
        if not location:
            return "", ""
        
        # Try to match IE cities and counties
        location_lower = location.lower()
        
        # Check counties
        county = ""
        if "san bernardino" in location_lower:
            county = "San Bernardino"
        elif "riverside" in location_lower:
            county = "Riverside"
        
        # Check cities
        city = ""
        for county_name, cities in self.ie_cities.items():
            for city_name in cities:
                if city_name in location_lower:
                    city = city_name.title()
                    if not county:
                        county = county_name.title()
                    break
        
        return city, county

    def _extract_date(self, soup, field_name: str) -> Optional[datetime]:
        """Extract and parse date fields"""
        date_str = self._extract_field(soup, field_name)
        if not date_str:
            return None
        
        # Try common date formats
        date_formats = ["%m/%d/%Y", "%Y-%m-%d", "%B %d, %Y"]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str.strip(), fmt)
            except ValueError:
                continue
        
        logger.debug(f"Could not parse date: {date_str}")
        return None

    def _extract_document_urls(self, soup) -> List[str]:
        """Extract PDF and document URLs"""
        urls = []
        
        # Look for PDF links
        pdf_links = soup.find_all('a', href=re.compile(r'\.pdf$', re.I))
        for link in pdf_links:
            href = link.get('href')
            if href:
                if href.startswith('http'):
                    urls.append(href)
                else:
                    urls.append(f"https://ceqanet.lci.ca.gov{href}")
        
        return urls

    def _classify_warehouse(self, project: CEQAProject):
        """Determine if project is warehouse-related and confidence score"""
        text_to_analyze = f"{project.title} {project.project_description}".lower()
        
        confidence = 0.0
        keywords_found = []
        
        # Check high confidence keywords
        for keyword in self.warehouse_keywords['high_confidence']:
            if keyword in text_to_analyze:
                confidence += 0.3
                keywords_found.append(keyword)
        
        # Check medium confidence keywords
        for keyword in self.warehouse_keywords['medium_confidence']:
            if keyword in text_to_analyze:
                confidence += 0.15
                keywords_found.append(keyword)
        
        # Check low confidence keywords
        for keyword in self.warehouse_keywords['low_confidence']:
            if keyword in text_to_analyze:
                confidence += 0.05
                keywords_found.append(keyword)
        
        project.warehouse_confidence = min(confidence, 1.0)
        project.is_warehouse = confidence >= 0.3  # Threshold for classification
        project.detection_keywords = keywords_found
        
        if project.is_warehouse:
            logger.info(f"Classified as warehouse: {project.title} (confidence: {confidence:.2f})")

    def _geocode_project(self, project: CEQAProject):
        """Get latitude/longitude for project address"""
        if not project.address:
            return
        
        try:
            # Try geocoding with full address
            location = self.geocoder.geocode(f"{project.address}, {project.city}, {project.county} County, CA")
            
            if location:
                project.latitude = location.latitude
                project.longitude = location.longitude
                logger.debug(f"Geocoded: {project.title}")
            else:
                # Fallback: try just city
                location = self.geocoder.geocode(f"{project.city}, CA")
                if location:
                    project.latitude = location.latitude
                    project.longitude = location.longitude
                    logger.debug(f"Geocoded (city fallback): {project.title}")
            
            time.sleep(1)  # Rate limiting for geocoding service
            
        except GeocoderTimedOut:
            logger.warning(f"Geocoding timeout for: {project.title}")
        except Exception as e:
            logger.warning(f"Geocoding error for {project.title}: {e}")

    def save_to_database(self, projects: List[CEQAProject]):
        """Save scraped projects to Supabase database"""
        logger.info(f"Saving {len(projects)} projects to database")
        
        for project in projects:
            try:
                # Map CEQA document type to UI status
                ui_status = self._map_ui_status(project.document_type)
                
                # Prepare data for insertion
                data = {
                    'title': project.title,
                    'lead_agency': project.lead_agency,
                    'city': project.city,
                    'county': project.county,
                    'address': project.address,
                    'latitude': project.latitude,
                    'longitude': project.longitude,
                    'project_description': project.project_description,
                    'project_type': project.project_type,
                    'document_type': project.document_type,
                    'ceqa_status': project.ceqa_status,
                    'ui_status': ui_status,
                    'date_posted': project.date_posted.isoformat() if project.date_posted else None,
                    'comment_deadline': project.comment_deadline.isoformat() if project.comment_deadline else None,
                    'ceqa_url': project.ceqa_url,
                    'document_urls': project.document_urls,
                    'is_warehouse': project.is_warehouse,
                    'warehouse_confidence': project.warehouse_confidence,
                    'detection_keywords': project.detection_keywords or [],
                    'scrape_date': datetime.now().date().isoformat()
                }
                
                # Insert or update (upsert on ceqa_url)
                result = self.supabase.table('warehouse_projects').upsert(
                    data, 
                    on_conflict='ceqa_url'
                ).execute()
                
                if project.is_warehouse:
                    logger.info(f"Saved warehouse project: {project.title}")
                
            except Exception as e:
                logger.error(f"Error saving project {project.title}: {e}")

    def _map_ui_status(self, document_type: str) -> str:
        """Map CEQA document types to UI-friendly statuses"""
        mapping = {
            'NOP': 'Proposal',
            'IS/MND': 'Under Review',
            'DEIR': 'Under Review',
            'NOD': 'Approved',
            'FEIR': 'Approved'
        }
        return mapping.get(document_type, 'Proposal')

    def run_scraping_job(self, max_projects: int = None):
        """Main scraping workflow"""
        try:
            logger.info("Starting CEQA warehouse scraping job")
            
            # Setup
            self.setup_driver(headless=True)
            
            # Navigate and search
            self.navigate_to_ceqa_search()
            self.configure_search_filters()
            self.submit_search()
            
            # Get project URLs
            project_urls = self.extract_project_links()
            
            if max_projects:
                project_urls = project_urls[:max_projects]
            
            # Scrape individual projects
            projects = []
            for i, url in enumerate(project_urls, 1):
                logger.info(f"Scraping project {i}/{len(project_urls)}")
                project = self.scrape_project_details(url)
                if project:
                    projects.append(project)
                
                # Rate limiting
                time.sleep(2)
            
            # Filter for warehouse projects
            warehouse_projects = [p for p in projects if p.is_warehouse]
            logger.info(f"Found {len(warehouse_projects)} warehouse projects out of {len(projects)} total")
            
            # Save to database
            if projects:
                self.save_to_database(projects)
            
            logger.info("Scraping job completed successfully")
            return projects
            
        except Exception as e:
            logger.error(f"Scraping job failed: {e}")
            raise
        finally:
            if self.driver:
                self.driver.quit()

def main():
    """Main execution function"""
    scraper = CEQAScraper()
    
    # Run scraping (limit to 50 for testing)
    projects = scraper.run_scraping_job(max_projects=50)
    
    # Print summary
    warehouse_count = len([p for p in projects if p.is_warehouse])
    print(f"\nScraping Summary:")
    print(f"Total projects scraped: {len(projects)}")
    print(f"Warehouse projects found: {warehouse_count}")
    print(f"Success rate: {warehouse_count/len(projects)*100:.1f}%")

if __name__ == "__main__":
    main()
