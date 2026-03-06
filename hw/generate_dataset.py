#!/usr/bin/env python3
"""
Generate a synthetic retail transactions dataset similar to Online Retail dataset
This creates ~1M rows with realistic transaction data
"""

import csv
import random
from datetime import datetime, timedelta

# Configuration
NUM_ROWS = 1_000_000
OUTPUT_FILE = "OnlineRetail.csv"

# Data generation parameters
COUNTRIES = ["United Kingdom", "Germany", "France", "Spain", "Belgium", "Switzerland", 
             "Portugal", "Austria", "Netherlands", "Italy", "Norway", "Sweden", "Denmark"]
PRODUCTS = [
    ("10002", "INFLATABLE POLITICAL GLOBE"),
    ("10120", "DOGGY RUBBER"),
    ("10123C", "HEARTS WRAPPING TAPE"),
    ("10125", "MINI FUNKY DESIGN CAKE CASES"),
    ("10133", "COLOURING PENCILS BROWN TUBE"),
    ("10135", "COLOURING PENCILS TUBE SKULLS"),
    ("11001", "ASSTD DESIGN RACING CARS"),
    ("15030", "ASSORTED COLOURS SILK FAN"),
    ("15034", "BLUE POLKADOT WRAP"),
    ("15036", "ASSORTED COLOUR BIRD ORNAMENT"),
    ("16014", "SMALL CHINESE STYLE SCISSOR"),
    ("17003", "BROCADE RING PURSE"),
    ("20665", "RED RETROSPOT CHARLOTTE BAG"),
    ("20719", "WOODLAND CHARLOTTE BAG"),
    ("20725", "LUNCH BAG RED RETROSPOT"),
    ("20727", "LUNCH BAG BLACK SKULL"),
    ("20728", "LUNCH BAG CARS BLUE"),
    ("21212", "PACK OF 72 RETROSPOT CAKE CASES"),
    ("21232", "STRAWBERRY CHARLOTTE BAG"),
    ("21754", "HOME BUILDING BLOCK WORD"),
    ("21755", "LOVE BUILDING BLOCK WORD"),
    ("21791", "VINTAGE HEADS AND TAILS CARD GAME"),
    ("21915", "RED HARMONICA IN BOX"),
    ("22086", "PAPER CHAIN KIT 50'S CHRISTMAS"),
    ("22111", "SCOTTIE DOG HOT WATER BOTTLE"),
    ("22112", "CHOCOLATE HOT WATER BOTTLE"),
    ("22139", "RETROSPOT TEA SET CERAMIC 11 PC"),
    ("22197", "SMALL POPCORN HOLDER"),
    ("22423", "REGENCY CAKESTAND 3 TIER"),
    ("22457", "NATURAL SLATE HEART CHALKBOARD"),
    ("22469", "HEART OF WICKER SMALL"),
    ("22492", "MINI PAINT SET VINTAGE"),
    ("22616", "PACK OF 12 LONDON TISSUES"),
    ("22622", "SET OF 4 KNICK KNACK TINS POPPIES"),
    ("22623", "BOX OF VINTAGE JIGSAW BLOCKS"),
    ("22666", "RECIPE BOX PANTRY YELLOW DESIGN"),
    ("22720", "SET OF 3 CAKE TINS PANTRY DESIGN"),
    ("22745", "POPPY'S PLAYHOUSE BEDROOM"),
    ("22747", "POPPY'S PLAYHOUSE KITCHEN"),
    ("22866", "HAND WARMER UNION JACK"),
    ("22910", "PAPER CHAIN KIT VINTAGE CHRISTMAS"),
    ("22960", "JAM MAKING SET WITH JARS"),
    ("23084", "RABBIT NIGHT LIGHT"),
    ("23166", "MEDIUM CERAMIC TOP STORAGE JAR"),
    ("23355", "HOT WATER BOTTLE KEEP CALM"),
    ("23843", "PAPER CRAFT LITTLE BIRDIE"),
    ("47566", "PARTY BUNTING"),
    ("84029G", "KNITTED UNION FLAG HOT WATER BOTTLE"),
    ("84879", "ASSORTED COLOUR BIRD ORNAMENT"),
]

START_DATE = datetime(2010, 12, 1)
END_DATE = datetime(2011, 12, 9)

def random_date(start, end):
    """Generate a random datetime between start and end"""
    delta = end - start
    random_days = random.randint(0, delta.days)
    random_seconds = random.randint(0, 86400)
    return start + timedelta(days=random_days, seconds=random_seconds)

def generate_invoice_no():
    """Generate invoice number"""
    return str(random.randint(536365, 581587))

def generate_customer_id():
    """Generate customer ID (some can be missing)"""
    if random.random() < 0.75:
        return str(random.randint(12346, 18287))
    return ""

print(f"Generating {NUM_ROWS:,} rows of retail transaction data...")

with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    
    writer.writerow([
        "InvoiceNo", "StockCode", "Description", "Quantity", 
        "InvoiceDate", "UnitPrice", "CustomerID", "Country"
    ])
    
    for i in range(NUM_ROWS):
        if (i + 1) % 100000 == 0:
            print(f"Generated {i + 1:,} rows...")
        
        stock_code, description = random.choice(PRODUCTS)
        quantity = random.randint(1, 50)
        unit_price = round(random.uniform(0.5, 50.0), 2)
        invoice_date = random_date(START_DATE, END_DATE).strftime("%m/%d/%Y %H:%M")
        
        writer.writerow([
            generate_invoice_no(),
            stock_code,
            description,
            quantity,
            invoice_date,
            unit_price,
            generate_customer_id(),
            random.choice(COUNTRIES)
        ])

print(f"✓ Dataset generated: {OUTPUT_FILE}")
print(f"Total rows: {NUM_ROWS:,}")
