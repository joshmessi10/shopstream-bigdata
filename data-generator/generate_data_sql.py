import json
import random
import os
from datetime import datetime, timedelta
import uuid
import calendar
from faker import Faker
fake = Faker()

# -------------------- CONFIGURATION --------------------
NUM_USERS = 50000
NUM_PRODUCTS = 90
START_DATE = datetime(2020, 1, 1)
END_DATE = datetime(2021, 12, 31)  # limit to 2020-2021
OUTPUT_DIR = "s3_raw_bucket"
# ------------------------------------------------------

# Clean output folder
if os.path.exists(OUTPUT_DIR):
    import shutil
    shutil.rmtree(OUTPUT_DIR)

# -----------------------------------------------------------------
# 1) STATIC DATA (users, products)
# -----------------------------------------------------------------
print("Generating users and products...")

users = []
for i in range(1, NUM_USERS + 1):
    uid = fake.unique.user_name()
    users.append({
        "user_id": uid,
        "username": fake.unique.user_name(),
        "country": fake.country()
    })

products = []
categories = ["electronics", "clothing", "home", "sports", "books", "beauty", "toys", "garden", "automotive", "health", "music", "office"]

for pid in range(1, NUM_PRODUCTS + 1):
    products.append({
        "product_id": f"prod_{pid:03d}",
        "product_name": fake.catch_phrase(),
        "category": random.choice(categories),
        "price": round(random.uniform(10, 1000), 2)
    })

# -----------------------------------------------------------------
# 2) FUNCTION TO WRITE EVENTS (KEY CHANGE)
# -----------------------------------------------------------------
from collections import defaultdict
event_buffer = defaultdict(list)

def write_event(event):
    dt = datetime.fromisoformat(event["timestamp"])

    year_dir = f"year={dt.year}"
    month_dir = calendar.month_name[dt.month]
    path = os.path.join(
        OUTPUT_DIR,
        year_dir,
        month_dir,
        f"{dt.day:02d}",
        event["event_type"]
    )

    file_path = os.path.join(path, "data.json")
    event_buffer[file_path].append(json.dumps(event) + "\n")

def flush_events():
    for file_path, lines in event_buffer.items():
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "a") as f:
            f.writelines(lines)
    event_buffer.clear()

# -----------------------------------------------------------------
# 3) EVENT GENERATION
# -----------------------------------------------------------------

device_types = ["mobile", "desktop", "tablet"]
page_types = ["home", "category", "product", "cart", "checkout"]
referrers = ["google", "direct", "social"]
# Generate a pool of distinct page URLs grouped by type for realistic navigation
url_pool = {
    "home": ["https://shopstream.com/home"],
    "category": [f"https://shopstream.com/category/{fake.unique.word()}" for _ in range(10)],
    "product": [f"https://shopstream.com/product/{fake.unique.word()}" for _ in range(30)],
    "cart": ["https://shopstream.com/cart"],
    "checkout": ["https://shopstream.com/checkout"]
}

session_counter = 1

current_date = START_DATE

print("Generating events...")

while current_date <= END_DATE:

    # usuarios activos ese día (reducido para llegar a ~500k eventos en total)
    active_users = random.sample(users, k=random.randint(100, 200))

    for user in active_users:

        sessions_per_user = random.randint(1, 2)

        for _ in range(sessions_per_user):

            session_id = session_counter
            session_counter += 1

            device = random.choice(device_types)

            start_dt = current_date + timedelta(
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59)
            )

            time_ptr = start_dt

            # secuencia realista: rebote vs navegación completa
            is_bounce = random.random() < 0.3
            
            events_to_create = []
            
            if is_bounce:
                events_to_create.append(("page_view", random.choice(["home", "category", "product"])))
            else:
                events_to_create.append(("page_view", "home"))
                # Add click event with a probability of 40%
                if random.random() < 0.4:
                    events_to_create.append(("click", None))
                
                if random.random() < 0.6:
                    events_to_create.append(("search", None))
                    events_to_create.append(("page_view", "category"))
                    
                if random.random() < 0.8:
                    events_to_create.append(("product_view", None))
                    
                    if random.random() < 0.5:
                        events_to_create.append(("cart_event", "add"))
                        
                        if random.random() < 0.4:
                            events_to_create.append(("page_view", "checkout"))

            for ev_type, detail in events_to_create:
                event = {
                    "event_type": ev_type,
                    "user_id": user["user_id"],
                    "session_id": session_id,
                    "timestamp": time_ptr.isoformat()
                }

                if ev_type == "page_view":
                    event.update({
                        "page_url": random.choice(url_pool[detail]),
                        "page_type": detail,
                        "time_on_page_seconds": random.randint(5, 120),
                        "referrer": random.choice(referrers),
                        "device_type": device,
                        "country": user["country"]
                    })

                elif ev_type == "click":
                    event.update({
                        "element_id": f"elem_{random.randint(1,100)}",
                        "element_type": "button",
                        "page_url": random.choice(url_pool["home"]),
                        "x_position": random.randint(0,1920),
                        "y_position": random.randint(0,1080)
                    })

                elif ev_type == "search":
                    event.update({
                        "query": fake.word(),
                        "results_count": random.randint(0,50)
                    })

                elif ev_type == "product_view":
                    prod = random.choice(products)
                    event.update({
                        "product_id": prod["product_id"],
                        "product_name": prod["product_name"],
                        "category": prod["category"],
                        "price": prod["price"],
                        "time_on_page_seconds": random.randint(10,300)
                    })

                elif ev_type == "cart_event":
                    # prod variable is either from product_view or a random one
                    prod = prod if 'prod' in locals() else random.choice(products)
                    event.update({
                        "product_id": prod["product_id"],
                        "product_name": prod["product_name"],
                        "category": prod["category"],
                        "price": prod["price"],
                        "action": detail
                    })

                write_event(event)

                # avanzar tiempo dentro de la sesión
                time_ptr += timedelta(seconds=random.randint(5, 60))

    if current_date.day == 1:
        print(f"{current_date.strftime('%Y-%m')} generated")

    flush_events()
    current_date += timedelta(days=1)

print("Data generation completed.")
