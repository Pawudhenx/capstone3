CREATE TABLE IF NOT EXISTS members (
  member_id SERIAL PRIMARY KEY,
  full_name VARCHAR(120) NOT NULL,
  email VARCHAR(160) UNIQUE NOT NULL,
  phone VARCHAR(40),
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS books (
  book_id SERIAL PRIMARY KEY,
  title VARCHAR(200) NOT NULL,
  author VARCHAR(120) NOT NULL,
  category VARCHAR(80),
  published_year INT CHECK (published_year BETWEEN 1900 AND EXTRACT(YEAR FROM NOW())),
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS loans (
  loan_id SERIAL PRIMARY KEY,
  member_id INT NOT NULL REFERENCES members(member_id),
  book_id INT NOT NULL REFERENCES books(book_id),
  loan_date DATE NOT NULL,
  due_date DATE NOT NULL,
  return_date DATE,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
