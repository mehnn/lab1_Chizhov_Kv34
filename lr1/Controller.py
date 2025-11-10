import time
import View
import Model

# підстав свої дані підключення
model = Model.db_model("postgres", "postgres", "123", "localhost")

while True:
    View.hello()
    choice = input("Оберіть варіант: ").strip()

    if choice == "0":
        break

    try:
        match choice:
            case "1":
                tables = model.get_table_names()
                View.show(tables)
                time.sleep(3)

            case "2":
                table = input("Введіть назву таблиці: ").strip()
                res = model.get_column_types(table)
                View.show(res)
                time.sleep(4)

            case "3":
                table = input("Введіть назву таблиці: ").strip()
                res = model.get_column_names(table)
                View.show(res)
                time.sleep(4)

            case "4":
                table = input("Введіть назву таблиці: ").strip()
                res = model.get_foreign_key_info(table)
                View.show(res)
                time.sleep(4)

            case "5":
                table = input("Введіть назву таблиці: ").strip()
                count = input("Скільки рядків згенерувати?: ").strip()
                model.generate_data(table, count)
                View.show(model.get_table_data(table))
                time.sleep(4)

            case "6":
                table = input("Введіть назву таблиці: ").strip()
                cols = model.get_column_names(table)
                print("Введіть значення для колонок (порожнє — пропустити):")
                values = {}
                for c in cols:
                    values[c] = input(f"{c}: ").strip()
                model.insert_data(table, values)
                print("Результат:")
                View.show(model.get_table_data(table))
                time.sleep(4)

            case "7":
                table = input("Введіть назву таблиці: ").strip()
                cols = model.get_column_names(table)
                print("Введіть нові значення (порожнє — не змінювати):")
                values = {}
                for c in cols:
                    values[c] = input(f"{c}: ").strip()
                condition = input("Умова (наприклад: client_id = 1): ").strip()
                values["condition"] = condition
                model.change_data(table, values)
                View.show(model.get_table_data(table))
                time.sleep(4)

            case "8":
                table = input("Введіть назву таблиці: ").strip()
                col = input("Назва колонки для умови: ").strip()
                param = input("Значення: ").strip()
                model.delete_data(table, col, param)
                View.show(model.get_table_data(table))
                time.sleep(4)

            case "9":
                table = input("Введіть назву таблиці: ").strip()
                View.show(model.get_table_data(table))
                time.sleep(4)

            case "10":
                table = input("Введіть назву таблиці: ").strip()
                model.delete_all_data(table)
                View.show(model.get_table_data(table))
                time.sleep(4)

            # ======== НОВІ ПУНКТИ З ЗАПИТАМИ ========

            case "11":
                # ЗВІТ 1: скільки бронювань у кожного клієнта за період
                date_from = input("Початкова дата (YYYY-MM-DD): ").strip()
                date_to = input("Кінцева дата (YYYY-MM-DD): ").strip()

                client_tbl = model.get_real_table_name("client")
                booking_tbl = model.get_real_table_name("booking1")

                query = f"""
                SELECT c.client_id,
                       c.client_name,
                       c.client_pasport,
                       COUNT(b.booking_id) AS bookings_count
                FROM {client_tbl} c
                JOIN {booking_tbl} b ON c.client_id = b.client_id
                WHERE b.date_in BETWEEN '{date_from}' AND '{date_to}'
                GROUP BY c.client_id, c.client_name, c.client_pasport
                ORDER BY bookings_count DESC;
                """

                t1 = time.time()
                model._db_model__cursor.execute(query)  # доступ до курсора через name-mangling
                rows = model._db_model__cursor.fetchall()
                t2 = time.time()
                elapsed_ms = (t2 - t1) * 1000

                print(("client_id", "client_name", "client_pasport", "bookings_count"))
                for r in rows:
                    print(r)
                print(f"\nЧас виконання: {elapsed_ms:.3f} мс")
                input("Enter...")

            case "12":
                # ЗВІТ 2: сума бронювань по ліжках і днях, фільтр за мінімальною ціною
                min_price = input("Мінімальна ціна бронювання (число): ").strip()

                bed_tbl = model.get_real_table_name("bed1")
                booking_tbl = model.get_real_table_name("booking1")

                query = f"""
                SELECT bd.bed_tier,
                       bd.bed_id,
                       b.date_in::date AS day,
                       SUM(b.booking_price) AS total_sum
                FROM {bed_tbl} bd
                JOIN {booking_tbl} b ON bd.bed_id = b.bed_id
                WHERE b.booking_price >= {min_price}
                GROUP BY bd.bed_tier, bd.bed_id, day
                ORDER BY bd.bed_tier, day;
                """

                t1 = time.time()
                model._db_model__cursor.execute(query)
                rows = model._db_model__cursor.fetchall()
                t2 = time.time()
                elapsed_ms = (t2 - t1) * 1000

                print(("bed_tier", "bed_id", "day", "total_sum"))
                for r in rows:
                    print(r)
                print(f"\nЧас виконання: {elapsed_ms:.3f} мс")
                input("Enter...")

            case "13":
                # ЗВІТ 3: платежі за методом у період
                pay_method = input("Метод оплати (cash/card/transfer): ").strip()
                date_from = input("Початкова дата (YYYY-MM-DD): ").strip()
                date_to = input("Кінцева дата (YYYY-MM-DD): ").strip()

                payment_tbl = model.get_real_table_name("payment1")
                booking_tbl = model.get_real_table_name("booking1")
                client_tbl = model.get_real_table_name("client")

                query = f"""
                SELECT p.method,
                       c.client_id,
                       c.client_name,
                       COUNT(*) AS payments_count
                FROM {payment_tbl} p
                JOIN {booking_tbl} b ON p.booking_id = b.booking_id
                JOIN {client_tbl} c ON b.client_id = c.client_id
                WHERE p.method = '{pay_method}'
                  AND b.date_in BETWEEN '{date_from}' AND '{date_to}'
                GROUP BY p.method, c.client_id, c.client_name
                ORDER BY payments_count DESC;
                """

                t1 = time.time()
                model._db_model__cursor.execute(query)
                rows = model._db_model__cursor.fetchall()
                t2 = time.time()
                elapsed_ms = (t2 - t1) * 1000

                print(("method", "client_id", "client_name", "payments_count"))
                for r in rows:
                    print(r)
                print(f"\nЧас виконання: {elapsed_ms:.3f} мс")
                input("Enter...")

            case _:
                print("Невірний пункт меню")
                time.sleep(2)

    except Exception as e:
        print(f"\nПомилка: {e}\n")
        model.clear_transaction()
        time.sleep(3)
