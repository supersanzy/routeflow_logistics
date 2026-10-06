"""
RouteFlow Logistics - Synthetic Shipment Data Generator
=========================================================

Generates a realistic-but-messy raw shipment CSV to seed a delivery-failure
data engineering pipeline (ingestion -> cleaning -> quarantine -> analytics).

Grain: one row = one shipment.

Columns:
    shipment_id      - unique shipment identifier (small % malformed/duplicated, never null)
    shipment_date    - date shipment was created (always valid, Jan-Dec 2026)
    delivery_date    - date of delivery attempt/result (only populated for
                        DELIVERED/FAILED shipments; ~9% of those are
                        intentionally bad: NULL, UNKNOWN, or wrong format)
    status           - DELIVERED (70%) / FAILED (15%) / other (15%), with
                        realistic case inconsistency (FAILED/failed/Failed)
    failure_reason   - logically derived from status (never random)
    carrier          - one of 5 carriers, each tied to a home region
    shipping_cost    - always clean, realistic USD value
    origin           - warehouse the shipment left from (always clean)
    destination      - drawn from the assigned carrier's regional pool,
                        so carrier <-> destination geography is realistic

Design notes / assumptions (stated so they can be revisited):
    - 2026 is not a leap year -> 365 days in the generation window.
    - "Bad data" that should get quarantined downstream is confined to two
      columns, per the project spec: shipment_id (small amount) and
      delivery_date (NULL / UNKNOWN / bad format only - never impossible
      calendar dates). Overall this nets out to ~8-10% of rows.
    - Status case-inconsistency (FAILED vs failed vs Failed) is a SEPARATE,
      always-cleanable data-quality issue (a standardization problem, not a
      validity problem), so it is not counted toward the quarantine %.
    - Rows with a status that hasn't been delivered/attempted yet
      (PENDING / IN_TRANSIT) legitimately have no delivery_date. That's a
      business-logic NULL, not a data-quality NULL, so it isn't counted as
      "bad" either.
"""

import os
import time
import numpy as np
import pandas as pd
from datetime import date, timedelta

# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------
OUTPUT_PATH = "routeflow_shipments_2026.csv"
TARGET_BYTES = int(1.5 * 1024 ** 3)   # ~1.5 GB
CHUNK_ROWS = 200_000
SEED = 42
START_DATE = date(2026, 1, 1)
YEAR_DAYS = 365                       # 2026 is not a leap year

BAD_ID_PROB = 0.015                   # ~1.5% of all rows get a malformed/duplicated id
DATE_BAD_PROB = 0.09                  # ~9% of *eligible* rows get a bad delivery_date

rng = np.random.default_rng(SEED)

# --------------------------------------------------------------------------
# Reference data: carriers <-> home regions (geographic realism)
# --------------------------------------------------------------------------
CARRIERS = {
    "RouteFlow Express": {
        "primary": ["New York, USA", "Los Angeles, USA", "Chicago, USA", "Houston, USA",
                    "Phoenix, USA", "Miami, USA", "Seattle, USA", "Denver, USA",
                    "Atlanta, USA", "Boston, USA"],
        "secondary": ["Toronto, Canada", "Vancouver, Canada"],
        "primary_weight": 0.92,
    },
    "EuroSwift Cargo": {
        "primary": ["London, UK", "Paris, France", "Berlin, Germany", "Madrid, Spain",
                    "Rome, Italy", "Amsterdam, Netherlands", "Warsaw, Poland",
                    "Vienna, Austria", "Stockholm, Sweden", "Lisbon, Portugal"],
        "secondary": ["Casablanca, Morocco", "Cairo, Egypt"],
        "primary_weight": 0.90,
    },
    "AsiaPacific Movers": {
        "primary": ["Tokyo, Japan", "Shanghai, China", "Singapore", "Seoul, South Korea",
                    "Mumbai, India", "Bangkok, Thailand", "Jakarta, Indonesia",
                    "Manila, Philippines", "Sydney, Australia", "Auckland, New Zealand"],
        "secondary": ["Dubai, UAE"],
        "primary_weight": 0.93,
    },
    "AfriConnect Logistics": {
        "primary": ["Lagos, Nigeria", "Nairobi, Kenya", "Johannesburg, South Africa",
                    "Accra, Ghana", "Addis Ababa, Ethiopia", "Kampala, Uganda",
                    "Dar es Salaam, Tanzania", "Dakar, Senegal"],
        "secondary": ["Dubai, UAE", "Cairo, Egypt"],
        "primary_weight": 0.88,
    },
    "TransGlobal Freight": {
        "primary": ["New York, USA", "London, UK", "Singapore", "Dubai, UAE",
                    "Tokyo, Japan", "Sydney, Australia", "Sao Paulo, Brazil",
                    "Johannesburg, South Africa", "Frankfurt, Germany", "Hong Kong"],
        "secondary": [],
        "primary_weight": 1.0,
    },
}
CARRIER_NAMES = list(CARRIERS.keys())
CARRIER_WEIGHTS = [0.28, 0.24, 0.22, 0.12, 0.14]   # relative shipment volume per carrier

WAREHOUSES = [
    "New York, USA", "Los Angeles, USA", "Chicago, USA",
    "London, UK", "Frankfurt, Germany",
    "Singapore", "Tokyo, Japan", "Mumbai, India",
    "Sydney, Australia", "Nairobi, Kenya", "Lagos, Nigeria", "Sao Paulo, Brazil",
]

STATUS_OTHER = ["PENDING", "IN_TRANSIT", "RETURNED", "CANCELLED"]
STATUS_OTHER_WEIGHTS = [0.40, 0.35, 0.15, 0.10]

FAILURE_REASONS = ["Incorrect address", "Vehicle breakdown", "Damaged package",
                    "Weather disruption", "Package lost"]
FAILURE_WEIGHTS = [0.32, 0.22, 0.20, 0.14, 0.12]

COLUMNS = ["shipment_id", "shipment_date", "delivery_date", "status",
           "failure_reason", "carrier", "shipping_cost", "origin", "destination"]


def messy_case(values: np.ndarray, rng: np.random.Generator) -> np.ndarray:
    """Apply realistic case inconsistency to an array of canonical (UPPER) status strings."""
    r = rng.random(len(values))
    out = values.copy()
    lower_mask = (r >= 0.55) & (r < 0.75)
    title_mask = (r >= 0.75) & (r < 0.90)
    cap_mask = r >= 0.90
    out[lower_mask] = np.char.lower(out[lower_mask].astype(str))
    out[title_mask] = np.array([v.title() for v in out[title_mask]])
    out[cap_mask] = np.array([v.capitalize() for v in out[cap_mask]])
    return out


def build_ids(n: int, start_counter: int, rng: np.random.Generator):
    idx = np.arange(start_counter, start_counter + n)
    clean_ids = np.array([f"S{i:07d}" for i in idx])
    ids = clean_ids.copy()

    bad_mask = rng.random(n) < BAD_ID_PROB
    bad_idx = np.where(bad_mask)[0]
    corruption_types = rng.integers(0, 4, size=len(bad_idx))

    for pos, ctype in zip(bad_idx, corruption_types):
        if ctype == 0:
            # missing "S" prefix
            ids[pos] = clean_ids[pos][1:]
        elif ctype == 1:
            # lowercase prefix
            ids[pos] = clean_ids[pos].lower()
        elif ctype == 2:
            # stray whitespace
            ids[pos] = f" {clean_ids[pos]} "
        else:
            # duplicate of the previous row's id (or next if pos == 0)
            dup_source = pos - 1 if pos > 0 else min(pos + 1, n - 1)
            ids[pos] = clean_ids[dup_source]

    return ids


def build_destinations(carriers: np.ndarray, rng: np.random.Generator) -> np.ndarray:
    n = len(carriers)
    destinations = np.empty(n, dtype=object)
    for name in CARRIER_NAMES:
        mask = carriers == name
        count = mask.sum()
        if count == 0:
            continue
        info = CARRIERS[name]
        primary, secondary = info["primary"], info["secondary"]
        pw = info["primary_weight"]
        if secondary:
            use_primary = rng.random(count) < pw
        else:
            use_primary = np.ones(count, dtype=bool)

        chosen = np.empty(count, dtype=object)
        n_primary = use_primary.sum()
        if n_primary > 0:
            chosen[use_primary] = rng.choice(primary, size=n_primary)
        n_secondary = count - n_primary
        if n_secondary > 0:
            chosen[~use_primary] = rng.choice(secondary, size=n_secondary)
        destinations[mask] = chosen
    return destinations


def generate_chunk(n: int, start_counter: int, rng: np.random.Generator) -> pd.DataFrame:
    # --- shipment_id ---
    shipment_id = build_ids(n, start_counter, rng)

    # --- status: 70% DELIVERED / 15% FAILED / 15% other ---
    u = rng.random(n)
    canonical_status = np.empty(n, dtype=object)
    mask_delivered = u < 0.70
    mask_failed = (u >= 0.70) & (u < 0.85)
    mask_other = u >= 0.85

    canonical_status[mask_delivered] = "DELIVERED"
    canonical_status[mask_failed] = "FAILED"
    n_other = mask_other.sum()
    if n_other > 0:
        canonical_status[mask_other] = rng.choice(
            STATUS_OTHER, size=n_other, p=STATUS_OTHER_WEIGHTS
        )
    status = messy_case(canonical_status, rng)

    # --- failure_reason: strictly derived from (canonical) status ---
    failure_reason = np.empty(n, dtype=object)
    failure_reason[mask_delivered] = "Delivery successful"
    failure_reason[mask_other] = "Not applicable"
    n_failed = mask_failed.sum()
    if n_failed > 0:
        failure_reason[mask_failed] = rng.choice(
            FAILURE_REASONS, size=n_failed, p=FAILURE_WEIGHTS
        )

    # --- shipment_date: always valid, uniform across the year ---
    ship_offset = rng.integers(0, YEAR_DAYS, size=n)
    shipment_date = np.array(
        [(START_DATE + timedelta(days=int(o))).isoformat() for o in ship_offset]
    )

    # --- delivery_date: only for DELIVERED/FAILED; ~9% of those are "bad" ---
    needs_date = mask_delivered | mask_failed
    delivery_date = np.full(n, "", dtype=object)  # legitimate blank for PENDING/IN_TRANSIT/etc.

    n_needs = needs_date.sum()
    idx_needs = np.where(needs_date)[0]
    lag_days = rng.integers(1, 10, size=n_needs)
    delivery_offset = np.minimum(ship_offset[idx_needs] + lag_days, YEAR_DAYS - 1)
    good_dates = np.array(
        [(START_DATE + timedelta(days=int(o))).isoformat() for o in delivery_offset]
    )
    delivery_date[idx_needs] = good_dates

    bad_mask_local = rng.random(n_needs) < DATE_BAD_PROB
    bad_positions = idx_needs[bad_mask_local]
    if len(bad_positions) > 0:
        bad_types = rng.integers(0, 4, size=len(bad_positions))
        for pos, btype, off in zip(bad_positions, bad_types, delivery_offset[bad_mask_local]):
            d = START_DATE + timedelta(days=int(off))
            if btype == 0:
                delivery_date[pos] = "NULL"
            elif btype == 1:
                delivery_date[pos] = "UNKNOWN"
            elif btype == 2:
                delivery_date[pos] = d.strftime("%m/%d/%Y")   # bad format: 03/17/2026
            else:
                delivery_date[pos] = d.strftime("%d-%m-%Y")   # bad format: 17-03-2026

    # --- carrier + geographically-consistent destination ---
    carrier = rng.choice(CARRIER_NAMES, size=n, p=CARRIER_WEIGHTS)
    destination = build_destinations(carrier, rng)

    # --- origin: company warehouse, always clean ---
    origin = rng.choice(WAREHOUSES, size=n)

    # --- shipping_cost: always clean ---
    shipping_cost = np.round(rng.uniform(5.0, 250.0, size=n), 2)

    return pd.DataFrame({
        "shipment_id": shipment_id,
        "shipment_date": shipment_date,
        "delivery_date": delivery_date,
        "status": status,
        "failure_reason": failure_reason,
        "carrier": carrier,
        "shipping_cost": shipping_cost,
        "origin": origin,
        "destination": destination,
    }, columns=COLUMNS)


def main():
    if os.path.exists(OUTPUT_PATH):
        os.remove(OUTPUT_PATH)

    counter = 0
    total_rows = 0
    start_time = time.time()
    first_chunk = True

    while True:
        df = generate_chunk(CHUNK_ROWS, counter, rng)
        df.to_csv(OUTPUT_PATH, mode="a", index=False, header=first_chunk)
        first_chunk = False
        counter += CHUNK_ROWS
        total_rows += len(df)

        size = os.path.getsize(OUTPUT_PATH)
        elapsed = time.time() - start_time
        print(f"rows={total_rows:,}  size={size / (1024**3):.3f} GB  elapsed={elapsed:.1f}s")

        if size >= TARGET_BYTES:
            break

    print("Done.")
    print(f"Total rows: {total_rows:,}")
    print(f"Final size: {os.path.getsize(OUTPUT_PATH) / (1024**3):.3f} GB")


if __name__ == "__main__":
    main()
