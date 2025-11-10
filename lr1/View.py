import os

clear = lambda: os.system('cls' if os.name == 'nt' else 'clear')


def hello():
    clear()
    print("[] Меню")
    print("1. Показати імена таблиць")
    print("2. Показати імена та типи стовпчиків таблиці")
    print("3. Показати лише імена стовпчиків таблиці")
    print("4. Показати зовнішні ключі таблиці")
    print("5. Згенерувати дані для таблиці")
    print("6. Вставити рядок у таблицю")
    print("7. Оновити рядок у таблиці")
    print("8. Видалити рядок з таблиці")
    print("9. Показати всі дані таблиці")
    print("10. Видалити всі дані таблиці")
    print("11. Звіт 1: бронювання клієнтів за період")
    print("12. Звіт 2: сума бронювань по ліжках")
    print("13. Звіт 3: платежі за методом")
    print("0. Вихід")


def show(mas):
    if isinstance(mas, tuple) and len(mas) == 2:
        headers, rows = mas

        if not headers:
            print("Порожньо.\n")
            return

        col_width = 15

        print("\n" + "-" * (len(headers) * (col_width + 3)))
        print(" | ".join(h.center(col_width) for h in headers))
        print("-" * (len(headers) * (col_width + 3)))

        for row in rows:
            formatted = []
            for value in row:
                if hasattr(value, "strftime"):
                    formatted.append(value.strftime("%Y-%m-%d"))
                else:
                    formatted.append(str(value))
            print(" | ".join(v.center(col_width) for v in formatted))

        print("-" * (len(headers) * (col_width + 3)) + "\n")

    else:
        for element in mas:
            print(element)
        print()
