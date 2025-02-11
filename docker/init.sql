CREATE TABLE IF NOT EXISTS customer_data (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100) UNIQUE,
    phone VARCHAR(20),
    address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO customer_data (name, email, phone, address) VALUES
('John Doe', 'john.doe@example.com', '123-456-7890', '123 Main St'), -- Duplikat email
('Emily Davis', 'emily.davis@example.com', '444-555-6666', '101 Maple St'),
('Michael Johnson', 'michael.johnson@example.com', '333-222-1111', '202 Birch St'),
('Olivia Martinez', 'olivia.martinez@example.com', '888-999-0000', '505 Redwood St'),
('Robert White', 'robert.white@example.com', '111-222-3333', '606 Cherry St'),
('Sophia Harris', 'sophia.harris@example.com', '222-333-4444', '707 Walnut St'),
('Daniel Clark', 'daniel.clark@example.com', '666-777-8888', '808 Ash St'),
('Sophia Harris', 'sophia.harris@example.com', '222-333-4444', '707 Walnut St'),
('Daniel Clark', 'daniel.clark@example.com', '666-777-8888', '808 Ash St'),;
