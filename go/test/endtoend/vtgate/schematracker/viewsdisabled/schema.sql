-- Schema for testing views disabled scenario
-- This schema includes both tables and views to test that VTGate
-- cannot load view definitions when EnableViews is disabled

CREATE TABLE users (
    id BIGINT NOT NULL,
    name VARCHAR(64) NOT NULL,
    email VARCHAR(128),
    status ENUM('active', 'inactive') DEFAULT 'active',
    PRIMARY KEY (id)
) ENGINE=InnoDB;

CREATE TABLE products (
    id BIGINT NOT NULL,
    name VARCHAR(128) NOT NULL,
    price DECIMAL(10,2),
    category VARCHAR(64),
    PRIMARY KEY (id)
) ENGINE=InnoDB;

CREATE TABLE orders (
    id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    quantity INT DEFAULT 1,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
) ENGINE=InnoDB;

-- Views that should NOT be loaded by VTGate when views are disabled
CREATE VIEW active_users AS 
SELECT id, name, email 
FROM users 
WHERE status = 'active';

CREATE VIEW expensive_products AS 
SELECT id, name, price, category 
FROM products 
WHERE price > 100.00;

CREATE VIEW user_orders AS 
SELECT 
    o.id as order_id,
    u.name as user_name,
    p.name as product_name,
    o.quantity,
    o.order_date
FROM orders o
JOIN users u ON o.user_id = u.id
JOIN products p ON o.product_id = p.id;

-- Insert some test data
INSERT INTO users (id, name, email, status) VALUES
(1, 'Alice Johnson', 'alice@example.com', 'active'),
(2, 'Bob Smith', 'bob@example.com', 'active'),
(3, 'Charlie Brown', 'charlie@example.com', 'inactive'),
(4, 'Diana Prince', 'diana@example.com', 'active');

INSERT INTO products (id, name, price, category) VALUES
(1, 'Laptop', 1299.99, 'Electronics'),
(2, 'Mouse', 29.99, 'Electronics'),
(3, 'Keyboard', 79.99, 'Electronics'),
(4, 'Monitor', 299.99, 'Electronics'),
(5, 'Coffee Mug', 15.99, 'Office');

INSERT INTO orders (id, user_id, product_id, quantity, order_date) VALUES
(1, 1, 1, 1, '2024-01-15 10:30:00'),
(2, 2, 2, 2, '2024-01-16 14:20:00'),
(3, 1, 4, 1, '2024-01-17 09:15:00'),
(4, 4, 3, 1, '2024-01-18 16:45:00');