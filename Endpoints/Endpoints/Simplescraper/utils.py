# epc_async_scraper/utils.py
import csv
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

def load_postcodes(file_path):
    with open(file_path, newline='') as csvfile:
        return [row[0].strip() for row in csv.reader(csvfile) if row]

def calculate_valid_from_date(valid_until_date):
    try:
        until = datetime.strptime(valid_until_date, "%d %B %Y")
        return (until - timedelta(days=365 * 10)).strftime("%Y-%m-%d %H:%M:%S")
    except:
        return None

def parse_results(html):
    soup = BeautifulSoup(html, 'html.parser')
    certificates = []
    rows = soup.select('.govuk-table__row')[1:]
    for row in rows:
        cols = row.select('td')
        if len(cols) >= 4:
            link = cols[0].select_one('a')
            cert = {
                'Address': link.get_text(strip=True) if link else '',
                'SourceUrl': f"https://find-energy-certificate.service.gov.uk{link['href']}" if link else '',
                'CertificateNumber': link['href'].split('/')[-1] if link else '',
                'Rating': cols[1].get_text(strip=True),
                'ValidUntilDate': cols[2].get_text(strip=True),
                'Expired': 1 if 'expired' in cols[2].get_text(strip=True).lower() else 0,
                'IsEmailSent': 0
            }
            certificates.append(cert)
    return certificates