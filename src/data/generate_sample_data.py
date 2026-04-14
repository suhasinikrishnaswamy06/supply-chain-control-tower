from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
import random

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = ROOT / "data" / "raw"


WAREHOUSES = [
    ("WH_TOR_01", "Toronto", "CA"),
    ("WH_DAL_01", "Dallas", "US"),
    ("WH_ATL_01", "Atlanta", "US"),
]

SKUS = [
    ("SKU_DRILL_100", "Power Drill", 120),
    ("SKU_SAW_200", "Circular Saw", 90),
    ("SKU_BATT_300", "Battery Pack", 160),
    ("SKU_TOOL_400", "Tool Kit", 70),
]

CARRIERS = ["DHL", "UPS", "FEDEX", "XPO"]
SHIPMENT_STATUSES = ["delivered", "delivered", "delivered", "in_transit", "delayed"]


@dataclass(frozen=True)
class OrderLine:
    order_id: str
    order_date: date
    warehouse_id: str
    sku_id: str
    quantity: int
    promised_date: date


def ensure_output_dir() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)


def generate_orders(n: int = 60) -> list[OrderLine]:
    today = date.today()
    orders: list[OrderLine] = []
    for idx in range(1, n + 1):
        warehouse_id, _, _ = random.choice(WAREHOUSES)
        sku_id, _, _ = random.choice(SKUS)
        order_date = today - timedelta(days=random.randint(2, 20))
        promised_date = order_date + timedelta(days=random.randint(2, 7))
        quantity = random.randint(5, 60)
        orders.append(
            OrderLine(
                order_id=f"ORD{idx:04d}",
                order_date=order_date,
                warehouse_id=warehouse_id,
                sku_id=sku_id,
                quantity=quantity,
                promised_date=promised_date,
            )
        )
    return orders


def build_orders_df(orders: list[OrderLine]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "order_id": [o.order_id for o in orders],
            "order_date": [o.order_date.isoformat() for o in orders],
            "warehouse_id": [o.warehouse_id for o in orders],
            "sku_id": [o.sku_id for o in orders],
            "order_qty": [o.quantity for o in orders],
            "promised_date": [o.promised_date.isoformat() for o in orders],
        }
    )


def build_shipments_df(orders: list[OrderLine]) -> pd.DataFrame:
    rows = []
    for idx, order in enumerate(orders, start=1):
        status = random.choice(SHIPMENT_STATUSES)
        shipped_qty = order.quantity if status != "delayed" else random.randint(0, order.quantity)
        ship_date = order.order_date + timedelta(days=random.randint(1, 4))
        actual_delivery = ship_date + timedelta(days=random.randint(1, 6))
        if status == "in_transit":
            actual_delivery_value = None
        else:
            actual_delivery_value = actual_delivery.isoformat()
        rows.append(
            {
                "shipment_id": f"SHP{idx:04d}",
                "order_id": order.order_id,
                "carrier_name": random.choice(CARRIERS),
                "warehouse_id": order.warehouse_id,
                "shipment_status": status,
                "ship_date": ship_date.isoformat(),
                "actual_delivery_date": actual_delivery_value,
                "shipped_qty": shipped_qty,
                "promised_date": order.promised_date.isoformat(),
            }
        )
    return pd.DataFrame(rows)


def build_inventory_df() -> pd.DataFrame:
    snapshot_date = date.today().isoformat()
    rows = []
    for warehouse_id, _, _ in WAREHOUSES:
        for sku_id, _, reorder_point in SKUS:
            on_hand = random.randint(20, 220)
            rows.append(
                {
                    "snapshot_date": snapshot_date,
                    "warehouse_id": warehouse_id,
                    "sku_id": sku_id,
                    "on_hand_qty": on_hand,
                    "reorder_point": reorder_point,
                }
            )
    return pd.DataFrame(rows)


def build_warehouse_events_df(orders: list[OrderLine]) -> pd.DataFrame:
    rows = []
    for idx, order in enumerate(orders, start=1):
        event_start = datetime.combine(order.order_date + timedelta(days=1), time(hour=8))
        delay_minutes = random.randint(5, 210)
        rows.append(
            {
                "event_id": f"EVT{idx:04d}",
                "order_id": order.order_id,
                "warehouse_id": order.warehouse_id,
                "event_type": "pick_started",
                "event_ts": (event_start + timedelta(minutes=delay_minutes)).isoformat(),
                "pick_sla_minutes": 60,
                "actual_pick_delay_minutes": delay_minutes,
            }
        )
    return pd.DataFrame(rows)


def main() -> None:
    random.seed(7)
    ensure_output_dir()

    orders = generate_orders()
    datasets = {
        "orders.csv": build_orders_df(orders),
        "shipments.csv": build_shipments_df(orders),
        "inventory_snapshots.csv": build_inventory_df(),
        "warehouse_events.csv": build_warehouse_events_df(orders),
    }

    for file_name, dataframe in datasets.items():
        dataframe.to_csv(RAW_DIR / file_name, index=False)
        print(f"Wrote {file_name} with {len(dataframe)} rows")


if __name__ == "__main__":
    main()
