#!/usr/bin/env python3
"""Générateur de commandes FastFood.ma

Usage :
  python producer.py --mode file  --n 50000            -> orders_history.jsonl
  python producer.py --mode kafka --rate 20            -> topic Kafka "orders"

Adresse Kafka : --bootstrap (défaut localhost:29092 depuis Windows,
                kafka:9092 depuis un conteneur Docker).
Simule ~5 % d'événements en retard (jusqu'à 15 min) et ~2 % de doublons.
"""
import argparse, json, os, random, sys, time
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
CITY_CODE = {"Casablanca": "CAS", "Rabat": "RAB", "Marrakech": "MAR", "Tanger": "TAN"}
LATE_RATE, DUP_RATE = 0.05, 0.02
N_CUSTOMERS = 3000


def load_restaurants(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


class Simulator:
    """Génère des événements de cycle de vie : CREATED -> PICKED_UP -> DELIVERED/CANCELLED."""

    def __init__(self, restaurants, seed=None):
        self.rng = random.Random(seed)
        self.restos = restaurants
        self.pending = []   # commandes en cours
        self.dups = []      # doublons à réémettre
        self.counter = 0

    def _new_order(self):
        r = self.rng.choice(self.restos)
        self.counter += 1
        items = self.rng.randint(1, 6)
        return {
            "order_id": f"ORD-{self.counter:07d}",
            "restaurant_id": r["restaurant_id"],
            "customer_id": f"C-{self.rng.randint(1, N_CUSTOMERS):05d}",
            "city": r["city"],
            "amount_mad": round(items * self.rng.uniform(25, 60) + 10, 2),
            "items_count": items,
            "distance_km": round(self.rng.uniform(0.5, 9.0), 1),
            "courier_id": f"L-{CITY_CODE[r['city']]}-{self.rng.randint(1, 20):02d}",
            "status": "CREATED",
        }

    def _emit(self, order, now):
        ev = dict(order)
        ts = now
        if self.rng.random() < LATE_RATE:           # événement en retard
            ts = now - timedelta(seconds=self.rng.uniform(60, 900))
        ev["event_time"] = iso(ts)
        if self.rng.random() < DUP_RATE:            # doublon
            self.dups.append(dict(ev))
        return ev

    def next_event(self, now):
        if self.dups:
            return self.dups.pop()
        if not self.pending or self.rng.random() < 0.4:
            o = self._new_order()
            self.pending.append(o)
            return self._emit(o, now)
        i = self.rng.randrange(len(self.pending))
        o = self.pending[i]
        p = self.rng.random()
        if o["status"] == "CREATED":
            o["status"] = "CANCELLED" if p < 0.07 else "PICKED_UP"
        else:
            o["status"] = "CANCELLED" if p < 0.03 else "DELIVERED"
        if o["status"] in ("DELIVERED", "CANCELLED"):
            self.pending.pop(i)
        if len(self.pending) > 3000:
            self.pending = self.pending[-2000:]
        return self._emit(o, now)


def run_file(args, sim):
    now = datetime.now(timezone.utc)
    t = now - timedelta(seconds=args.n * 0.5)
    with open(args.out, "w", encoding="utf-8") as f:
        for _ in range(args.n):
            t += timedelta(seconds=0.5)
            f.write(json.dumps(sim.next_event(t)) + "\n")
    print(f"{args.n} événements écrits dans {args.out}")


def run_kafka(args, sim):
    try:
        from kafka import KafkaProducer
    except ImportError:
        sys.exit("Installez kafka-python : python -m pip install kafka-python")
    prod = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        key_serializer=lambda k: k.encode(),
        value_serializer=lambda v: json.dumps(v).encode(),
    )
    print(f"Envoi vers {args.bootstrap} topic '{args.topic}' à {args.rate} évt/s (Ctrl+C pour arrêter)")
    n, delay = 0, 1.0 / args.rate
    try:
        while True:
            ev = sim.next_event(datetime.now(timezone.utc))
            prod.send(args.topic, key=ev["order_id"], value=ev)
            n += 1
            if n % 100 == 0:
                print(f"{n} événements envoyés", flush=True)
            time.sleep(delay)
    except KeyboardInterrupt:
        pass
    finally:
        prod.flush()
        print(f"Arrêt. Total envoyé : {n}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["file", "kafka"], required=True)
    ap.add_argument("--n", type=int, default=50000)
    ap.add_argument("--rate", type=float, default=20)
    ap.add_argument("--out", default="orders_history.jsonl")
    ap.add_argument("--topic", default="orders")
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092"))
    ap.add_argument("--restaurants", default=os.path.join(HERE, "restaurants.json"))
    ap.add_argument("--seed", type=int, default=None)
    args = ap.parse_args()
    sim = Simulator(load_restaurants(args.restaurants), args.seed)
    run_file(args, sim) if args.mode == "file" else run_kafka(args, sim)


if __name__ == "__main__":
    main()
