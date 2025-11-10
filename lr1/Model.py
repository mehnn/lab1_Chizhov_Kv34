import psycopg2
from psycopg2 import sql
import time


class db_model:
    def __init__(self, dbname, user_name, password, host="localhost"):
        # підключення до БД
        self.__context = psycopg2.connect(
            dbname=dbname,
            user=user_name,
            password=password,
            host=host
        )
        self.__cursor = self.__context.cursor()

    def __del__(self):
        self.__cursor.close()
        self.__context.close()

    def clear_transaction(self):
        """Відкотити невдалу транзакцію"""
        self.__context.rollback()

    # ================== ДОПОМІЖНЕ ==================
    def _table_exists(self, table_name: str) -> bool:
        self.__cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = %s
            )
            """,
            (table_name,)
        )
        return self.__cursor.fetchone()[0]

    def _resolve_table_name(self, table_name: str) -> str:
        """
        Вирівнюємо назви: bed -> bed1, booking -> booking1, payment -> payment1,
        але якщо в БД є саме те, що ввели — беремо це.
        """
        aliases = {
            "bed": "bed1",
            "booking": "booking1",
            "payment": "payment1",
        }
        candidate = aliases.get(table_name, table_name)

        if self._table_exists(candidate):
            return candidate

        # якщо закінчується на 1 — пробуємо без 1
        if candidate.endswith("1"):
            alt = candidate[:-1]
            if self._table_exists(alt):
                return alt

        # пробуємо додати 1
        alt = candidate + "1"
        if self._table_exists(alt):
            return alt

        raise Exception(f"Таблиця '{table_name}' не знайдена в БД")

    def get_real_table_name(self, table_name: str) -> str:
        return self._resolve_table_name(table_name)

    # ================== МЕТАДАНІ ==================
    def get_table_names(self):
        base = ["client", "bed1", "bed", "booking1", "booking", "payment1", "payment"]
        return [t for t in base if self._table_exists(t)]

    def get_column_types(self, table_name):
        table_name = self._resolve_table_name(table_name)
        self.__cursor.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name,)
        )
        return self.__cursor.fetchall()

    def get_column_names(self, table_name):
        table_name = self._resolve_table_name(table_name)
        self.__cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name,)
        )
        return [x[0] for x in self.__cursor.fetchall()]

    def get_foreign_key_info(self, table_name):
        table_name = self._resolve_table_name(table_name)
        self.__cursor.execute(
            """
            SELECT kcu.column_name,
                   ccu.table_name AS foreign_table_name,
                   ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                 ON tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                 ON ccu.constraint_name = tc.constraint_name
                 AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_name = %s;
            """,
            (table_name,)
        )
        return self.__cursor.fetchall()

    # ================== ЧИТАННЯ ==================
    def get_table_data(self, table_name):
        table_name = self._resolve_table_name(table_name)
        cols = self.get_column_types(table_name)
        if not cols:
            return ([], [])

        id_column = cols[0][0]
        self.__cursor.execute(
            sql.SQL("SELECT * FROM {} ORDER BY {} ASC")
            .format(sql.Identifier(table_name), sql.Identifier(id_column))
        )
        return ([col.name for col in self.__cursor.description], self.__cursor.fetchall())

    # ================== INSERT ==================
    def insert_data(self, table_name, values: dict):
        table_name = self._resolve_table_name(table_name)
        line = ''
        columns = '('
        for key in values:
            if values[key] != "":
                line += '%(' + key + ')s,'
                columns += key + ','
        columns = columns[:-1] + ')'
        query = sql.SQL('INSERT INTO {} {} VALUES (' + line[:-1] + ')') \
            .format(sql.Identifier(table_name), sql.SQL(columns))

        self.__cursor.execute(query, values)
        self.__context.commit()

    # ================== UPDATE ==================
    def change_data(self, table_name, values: dict):
        table_name = self._resolve_table_name(table_name)
        cond = values.pop('condition')
        line = ''
        for key in values:
            if values[key] != "":
                line += key + '=%(' + key + ')s,'

        query = sql.SQL('UPDATE {} SET ' + line[:-1] + ' WHERE {}') \
            .format(sql.Identifier(table_name), sql.SQL(cond))

        self.__cursor.execute(query, values)
        self.__context.commit()

    # ================== DELETE ==================
    def delete_data(self, table_name, column_name, param):
        table_name = self._resolve_table_name(table_name)
        query = sql.SQL('DELETE FROM {} WHERE {} = %s') \
            .format(sql.Identifier(table_name), sql.Identifier(column_name))
        self.__cursor.execute(query, (param,))
        self.__context.commit()

    def delete_all_data(self, table_name):
        table_name = self._resolve_table_name(table_name)
        self.__cursor.execute(
            sql.SQL('DELETE FROM {}').format(sql.Identifier(table_name))
        )
        self.__context.commit()

    # ================== ГЕНЕРАЦІЯ (як у прикладі з journal) ==================
    def generate_data(self, table_name, count):
        """
        Генерує дані для однієї з відомих таблиць:
        - client
        - bed / bed1
        - booking / booking1  (random client, random bed)
        - payment / payment1  (random booking)
        Усе робиться ОДНИМ SQL, без python-циклів.
        """
        table_name = self._resolve_table_name(table_name)
        count = int(count)

        # ---------- CLIENT ----------
        if table_name == "client":
            sql_query = """
                WITH maxv AS (
                    SELECT COALESCE(MAX(client_id), 0) AS m FROM client
                )
                INSERT INTO client (client_id, client_name, client_pasport, client_phone)
                SELECT
                    m + row_number() OVER () AS new_id,
                    chr(65 + (random()*25)::int) || chr(65 + (random()*25)::int) AS client_name,
                    (100000 + (random()*900000)::int) AS client_pasport,
                    (380000000 + (random()*999999)::int) AS client_phone
                FROM maxv, generate_series(1, %s);
            """
            self.__cursor.execute(sql_query, (count,))
            self.__context.commit()
            return

        # ---------- BED ----------
        if table_name in ("bed", "bed1"):
            real_bed = self._resolve_table_name("bed")
            sql_query = f"""
                WITH maxv AS (
                    SELECT COALESCE(MAX(bed_id), 0) AS m FROM {real_bed}
                )
                INSERT INTO {real_bed} (bed_id, bed_tier)
                SELECT
                    m + row_number() OVER () AS new_id,
                    (1 + (random()*3)::int) AS bed_tier
                FROM maxv, generate_series(1, %s);
            """
            self.__cursor.execute(sql_query, (count,))
            self.__context.commit()
            return

        # ---------- BOOKING ----------
        if table_name in ("booking", "booking1"):
            real_booking = self._resolve_table_name(table_name)
            real_bed = self._resolve_table_name("bed")      # з цієї таблиці беремо bed_id
            real_client = self._resolve_table_name("client")  # а з цієї client_id

            sql_query = f"""
                WITH maxv AS (
                    SELECT COALESCE(MAX(booking_id), 0) AS m FROM {real_booking}
                ),
                counts AS (
                    SELECT
                        (SELECT COUNT(*) FROM {real_bed})    AS beds_count,
                        (SELECT COUNT(*) FROM {real_client}) AS clients_count
                ),
                gens AS (
                    SELECT
                        m + row_number() OVER () AS new_id,
                        -- випадковий номер ліжка серед існуючих
                        (floor(random() * (SELECT beds_count FROM counts))::int + 1) AS bed_idx,
                        -- випадковий номер клієнта серед існуючих
                        (floor(random() * (SELECT clients_count FROM counts))::int + 1) AS client_idx,
                        -- випадкова дата 2017-2025
                        (
                            DATE '2017-01-01'
                            + (trunc(random() * (DATE '2025-12-31' - DATE '2017-01-01'))::int) * INTERVAL '1 day'
                        )::date AS d_in,
                        (200 + (random()*800)::int) AS price
                    FROM maxv, counts, generate_series(1, %s)
                )
                INSERT INTO {real_booking} (booking_id, bed_id, client_id, date_in, date_out, booking_price)
                SELECT
                    g.new_id,
                    b.bed_id,
                    c.client_id,
                    g.d_in,
                    g.d_in + (1 + (random()*13)::int) * INTERVAL '1 day' AS date_out,
                    g.price
                FROM gens g
                JOIN (
                    SELECT row_number() OVER (ORDER BY bed_id) AS idx, bed_id
                    FROM {real_bed}
                ) b ON b.idx = g.bed_idx
                JOIN (
                    SELECT row_number() OVER (ORDER BY client_id) AS idx, client_id
                    FROM {real_client}
                ) c ON c.idx = g.client_idx;
            """
            self.__cursor.execute(sql_query, (count,))
            self.__context.commit()
            return

        # ---------- PAYMENT ----------
        if table_name in ("payment", "payment1"):
            real_payment = self._resolve_table_name(table_name)
            real_booking = self._resolve_table_name("booking")

            sql_query = f"""
                WITH maxv AS (
                    SELECT COALESCE(MAX(payment_id), 0) AS m FROM {real_payment}
                ),
                counts AS (
                    SELECT (SELECT COUNT(*) FROM {real_booking}) AS bookings_count
                ),
                gens AS (
                    SELECT
                        m + row_number() OVER () AS new_id,
                        (floor(random() * (SELECT bookings_count FROM counts))::int + 1) AS booking_idx
                    FROM maxv, counts, generate_series(1, %s)
                )
                INSERT INTO {real_payment} (payment_id, booking_id, method)
                SELECT
                    g.new_id,
                    bk.booking_id,
                    CASE (trunc(random()*3))
                        WHEN 0 THEN 'cash'
                        WHEN 1 THEN 'card'
                        ELSE 'transfer'
                    END AS method
                FROM gens g
                JOIN (
                    SELECT row_number() OVER (ORDER BY booking_id) AS idx, booking_id
                    FROM {real_booking}
                ) bk ON bk.idx = g.booking_idx;
            """
            self.__cursor.execute(sql_query, (count,))
            self.__context.commit()
            return

        # якщо дійшли сюди — значить таблиця нам не відома
        raise Exception("Невідома таблиця для генерації (SQL)")

    # ================== JOIN (залишимо, як у тебе було) ==================
    def join_general(self, main_query, condition=""):
        new_cond = condition
        if condition:
            new_cond = "WHERE " + condition
        t1 = time.time()
        self.__cursor.execute(main_query.format(new_cond))
        t2 = time.time()
        return ((t2 - t1) * 1000, self.__cursor.fetchall())
