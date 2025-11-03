import csv, os
from datetime import datetime, timedelta, date
from random import randint, choice, random
import psycopg2

DSN = os.getenv("PG_DSN", "host=localhost user=postgres password=postgres dbname=library port=5432")

def insert_members(cur, path):
    with open(path, newline='', encoding='utf-8') as f:
        for r in csv.DictReader(f):
            cur.execute("""
                INSERT INTO members(full_name,email,phone,created_at)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT (email) DO NOTHING
            """, (r["full_name"], r["email"], r["phone"], datetime.utcnow()))

def insert_books(cur, path):
    with open(path, newline='', encoding='utf-8') as f:
        for r in csv.DictReader(f):
            cur.execute("""
                INSERT INTO books(title,author,category,published_year,created_at)
                VALUES (%s,%s,%s,%s,%s)
            """, (r["title"], r["author"], r["category"], int(r["published_year"]), datetime.utcnow()))

def insert_loans_random(cur, n=12):
    cur.execute("SELECT member_id FROM members"); member_ids = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT book_id FROM books"); book_ids = [r[0] for r in cur.fetchall()]
    today = date.today()
    for _ in range(n):
        m = choice(member_ids); b = choice(book_ids)
        loan_dt = today - timedelta(days=randint(0, 10))
        due_dt = loan_dt + timedelta(days=7 + randint(0, 7))
        return_dt = None if random() < 0.6 else (loan_dt + timedelta(days=randint(1, 14)))
        cur.execute("""
            INSERT INTO loans(member_id,book_id,loan_date,due_date,return_date,created_at)
            VALUES (%s,%s,%s,%s,%s,%s)
        """, (m, b, loan_dt, due_dt, return_dt, datetime.utcnow()))

def main():
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        insert_members(cur, os.path.join("..","data","members_seed.csv"))
        insert_books(cur, os.path.join("..","data","books_seed.csv"))
        insert_loans_random(cur, n=12)
        conn.commit()
    print("Done: members, books, loans inserted.")

if __name__ == "__main__":
    main()
