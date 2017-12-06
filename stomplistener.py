import stomp
import psycopg2

class StompListener(stomp.ConnectionListener):
    def __init__(self, conn):
        self.conn = conn
    def on_error(self, headers, message):
        print('received an error "%s"' % message)
    def on_message(self, headers, itemID):
        # Place in postgres
        cur = self.conn.cursor()
        cur.execute("""
            SELECT * FROM sales
            WHERE itemid = %s;
            """,
            (itemID,))
        if(cur.fetchone()==None):
            cur.execute("""
            INSERT INTO sales(itemid, quantity)
            VALUES(%s, %s);
            """,
            (itemID, 1))
        else:
            cur.execute("""
            UPDATE sales
            SET quantity = quantity + 1
            WHERE itemid = %s;
            """,
            (itemID,))
        self.conn.commit()
