import csv #To write data to a CSV file
import random #to generate random data for various fields
from datetime import datetime, timedelta #To generate random dates and handle date manipulations
import os #To handle file paths and directories in a platform-independent way

# -------------------------------
# Configuration
# -------------------------------
BASE_DIR = os.getcwd()            # Where PowerShell runs the script
 #os.getcwd() stands for get current working directory. It is a built-in function from the os module that returns
         #the absolute path of the folder where your Python process is currently executing
OUTPUT_DIR = os.path.join(BASE_DIR, "data/raw")
  #s.path.join(BASE_DIR, 'data/raw'): Combines your base directory with the subfolder path data/raw/ in an 
         # OS-independent way (handling both Windows \ and Mac/Linux / slashes automatically).
         #Accidental Absolute Paths: Never start your subfolders with a leading slash (e.g., '/data/raw'). 
         # If os.path.join() see a leading slash, it thinks it as an absolute path and completely discards BASE_DIR.
OUTPUT_FILE = "retail_sales_raw.csv"
NUM_RECORDS = 1_000_000

os.makedirs(OUTPUT_DIR, exist_ok=True)#: Creates the data/raw folder structure if it doesn't already exist.
file_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
 #: Combines your folder path and file name to create the full, absolute path pointing directly to where your CSV file will be saved

# -------------------------------
# Reference Data
# -------------------------------
cities = [                                                             # Top 10 US cities and their states
    ("New York", "NY"), ("Los Angeles", "CA"), ("Chicago", "IL"),  
    ("Houston", "TX"), ("Phoenix", "AZ"), ("Philadelphia", "PA"),
    ("San Antonio", "TX"), ("San Diego", "CA"), ("Dallas", "TX"),
    ("San Jose", "CA")
]

categories = {                                                             # Product categories and their price ranges
    "Electronics": (100, 2000),
    "Fashion": (20, 500),
    "Grocery": (1, 50),
    "Furniture": (50, 1500),
    "Sports": (10, 800)
}

payment_types = ["Card", "UPI", "COD", "Crypto", None]  # Payment methods including some faulty data
genders = ["M", "F", "Male", "Female", None]  # Gender values including some inconsistencies like only M,F allowed
order_statuses = ["Delivered", "Cancelled", "Returned"] 

start_date = datetime(2023, 1, 1)
end_date = datetime(2026, 1, 1)
# date between Jan 1, 2023 and Jan 1, 2026
# -------------------------------
# Helper Functions
# -------------------------------
# Function to generate a random date between start and end
def random_date(start, end): 
    delta = end - start    # Total number of days between start and end[which is 1095 for 3 years]
    return start + timedelta(days=random.randint(0, delta.days)) # Generate a random date within the specified range

# -------------------------------
# Data Generation
# -------------------------------
with open(file_path, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)

    writer.writerow([
        "transaction_id",
        "order_date",
        "ship_date",
        "customer_id",
        "customer_age",
        "gender",
        "product_id",
        "product_category",
        "quantity",
        "unit_price",
        "discount_pct",
        "city",
        "state",
        "payment_type",
        "order_status",
        "ingestion_date"
    ])

    for i in range(NUM_RECORDS):
        transaction_id = random.randint(1, NUM_RECORDS // 2)  # Introduce some duplicate transaction IDs as only 5lakhs unique IDs for 1 million/lakhs records
        order_date = random_date(start_date, end_date) 
        ship_date = order_date + timedelta(days=random.randint(-3, 10)) #-3 to -1 is not possible ship_date >= order_date
        # Introduce some records where ship date is before order date to simulate data quality issues

        customer_id = f"CUST{random.randint(1, 200_000)}" 
        customer_age = random.choice([
            random.randint(18, 70),
            random.randint(-10, 10), #Negative Age is unrealistic 
            random.randint(120, 200), #age between 120 and 200 is unrealistic
            None
        ])# Introduce some unrealistic ages and nulls to simulate data quality issues

        gender = random.choice(genders)
        category = random.choice(list(categories.keys())) #Get a product category from the list defined
        price_min, price_max = categories[category] #Get the price range for the selected category

        unit_price = random.choice([
            round(random.uniform(price_min, price_max), 2), #Generate a random price within the category's price range
            -random.uniform(1, 100), # Negative price is a anomaly
            None # Introduce some nulls to simulate data quality issues
        ])
        quantity = random.choice([
            random.randint(1, 10),
            0,
            -random.randint(1, 5) # Negative quantity is not real
        ])

        discount_pct = random.choice([
            round(random.uniform(0, 50), 2), # Realistic discount percentage between 0% and 50%[uniform(a,b)choose a random float between a and b,round(..,2) : Rounds that generated number to exactly 2 decimal places.]
            round(random.uniform(60, 150), 2),# Discount percentage above 50% is unusual but above 100% is unrealistic for most retail scenarios
            None #Note this is a faulty data as 0% is already taken care of in the first option
        ])# Introduce some unrealistic discount percentages and nulls 

        city, state = random.choice(cities)

        writer.writerow([
            transaction_id,
            order_date.strftime("%Y-%m-%d"),# strftime() stands for "string format time". It is used to convert a datetime or time object into a readable text string.
            ship_date.strftime("%Y-%m-%d"),
            customer_id,
            customer_age,
            gender,
            f"PROD{random.randint(1, 50_000)}", # Generate a product ID with 50,000 unique products
            category,
            quantity,
            unit_price,
            discount_pct,
            city,
            state,
            random.choice(payment_types),
            random.choice(order_statuses),
            datetime.now().strftime("%Y-%m-%d")  # Ingestion date is set to current date for all records
        ])

print(f"Generated {NUM_RECORDS} records at:\n{file_path}") # Print the location of the generated file for reference