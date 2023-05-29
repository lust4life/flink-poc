import psycopg2


def prepare_orders(cur):
    cur.execute(
        """
        create table if not exists orders
        (
            order_id           varchar(50) not null primary key,
            product_id         varchar(50),
            customer_id        varchar(50),
            purchase_timestamp timestamptz
        );

        ALTER TABLE public.orders REPLICA IDENTITY FULL;
        """
    )

    cur.execute("SELECT * FROM orders limit 1")
    any = cur.fetchone()
    if any:
        return

    return

    with open("./data/orders.csv", "r") as f:
        next(f)  # Skip the header row.
        cur.copy_from(f, "orders", sep=",")


def prepare_products(cur):
    cur.execute(
        """
        create table if not exists products
        (
            product_id     varchar(50) not null primary key,
            product_name   varchar(50),
            sale_price     integer,
            product_rating double precision
        );

        ALTER TABLE public.products REPLICA IDENTITY FULL;
        """
    )

    cur.execute("SELECT * FROM products limit 1")
    any = cur.fetchone()
    if any:
        return

    return

    with open("./data/products.csv", "r") as f:
        next(f)  # Skip the header row.
        cur.copy_from(f, "products", sep=",")


def prepare_customers(cur):
    cur.execute(
        """
        create table if not exists customers
        (
            customer_id     varchar(50) not null primary key,
            customer_name   varchar(50)
        );

        ALTER TABLE public.customers REPLICA IDENTITY FULL;
        """
    )

    cur.execute("SELECT * FROM customers limit 1")
    any = cur.fetchone()
    if any:
        return

    return

    with open("./data/customers.csv", "r") as f:
        next(f)  # Skip the header row.
        cur.copy_from(f, "customers", sep=",")


if __name__ == "__main__":
    conn = psycopg2.connect(
        "host=localhost dbname=postgres user=postgres password=postgres"
    )
    cur = conn.cursor()

    try:
        prepare_orders(cur)
        prepare_products(cur)
        prepare_customers(cur)
        conn.commit()
    except Exception as e:
        print(e)
        conn.rollback()

    conn.close()
