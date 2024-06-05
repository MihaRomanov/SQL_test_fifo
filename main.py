import sqlite3
import pandas as pd

connection = sqlite3.connect('data.db')
cursor = connection.cursor()

cursor.execute('''
DELETE FROM deals
''')
cursor.execute('''
CREATE TABLE IF NOT EXISTS deals (
    [date_oper] datetime,
    [client] varchar(64),
    [share] VARCHAR(20),
    [quantity] decimal(19,7),
    [price] decimal(19,7)
)
''')
cursor.execute('''
CREATE TABLE IF NOT EXISTS deals (
    [date_oper] datetime,
    [client] varchar(6),
    [share] varchar(4),
    [quantity] REAL(19,7),
    [price] REAL(19,7)
)
''')
connection.commit()

cursor.execute('''
INSERT INTO deals
VALUES
( '2022-01-13T17:36:32', '016084', 'VTBR', 15000000, 0.006013),
( '2022-01-13T17:36:37', '016084', 'VTBR', 10000000, 0.006014),
( '2022-01-13T17:36:39', '016084', 'VTBR', 10000000, 0.006015),
( '2022-01-13T17:36:40', '016084', 'VTBR', 7000000, 0.006012),
( '2022-01-13T21:23:07', '016084', 'VTBR', -40000000, 0.006020),
( '2022-01-13T21:23:10', '016084', 'VTBR', 1000000, 0.006016),
( '2022-01-13T21:23:12', '016084', 'VTBR', 10000000, 0.006018),
( '2022-01-13T21:23:18', '016084', 'VTBR', -1500000, 0.006012),
( '2022-01-13T21:23:19', '016084', 'VTBR', -1000000, 0.006013),
( '2022-01-13T21:23:26', '016084', 'VTBR', -500000, 0.006010),
( '2022-01-13T21:23:28', '016084', 'VTBR', -2000000, 0.006025),
( '2022-01-13T21:23:33', '016084', 'VTBR', -2000000, 0.006030),
( '2022-01-13T21:23:34', '016084', 'VTBR', -6000000, 0.006030),
('2022-01-14T10:10:34', '016085', 'ADT', 0.5367, 80.15),
('2022-01-14T11:15:18', '016085', 'ADT', 0.483, 81.15),
('2022-01-14T11:15:19', '016085', 'ADT', 0.283, 81.15),
('2022-01-14T12:15:20', '016085', 'ADT', -1.0889, 82.25),
('2022-01-14T12:15:21', '016085', 'ADT', -0.2138, 82.28);
''')
connection.commit()
cursor.execute('''
SELECT
    d.[date_oper],
    d.[client],
    d.[share],
    d.[quantity],
    d.[price],
    CASE
        WHEN d.[quantity] < 0 THEN d.[quantity] * d.[price]  -- Продажа
        ELSE 0  -- Покупка
    END AS [amount],
    SUM(CASE
        WHEN d.[quantity] < 0 THEN d.[quantity] * d.[price]  -- Продажа
        ELSE 0  -- Покупка
    END) OVER (PARTITION BY d.[share] ORDER BY d.[date_oper]) AS [profit]
FROM deals d
WHERE d.[quantity] < 0  -- Только продажи
ORDER BY d.[share], d.[date_oper]
''')
df = pd.read_sql_query('''
SELECT
    date_oper,
    client,
    share,
    quantity,
    price
FROM
    deals
''',connection)
df['profit'] = df['quantity'] * df['price']
pd.options.display.float_format = '{:.2f}'.format
profit_df = pd.DataFrame(columns=['date_oper', 'client', 'share', 'profit'])

# Создаем список для хранения информации о прибыли
profit_list = []

# Обрабатываем каждую акцию отдельно
for share in df['share'].unique():
    temp_df = df[df['share'] == share].sort_values(by='date_oper')
    temp_df['cumulative_quantity'] = temp_df['quantity'].cumsum()

    # Рассчитываем прибыль для каждой операции продажи
    for index, row in temp_df.iterrows():
        if row['quantity'] < 0:  # Это операция продажи
            # Находим все предыдущие операции покупки
            previous_purchases = temp_df[(temp_df['date_oper'] < row['date_oper']) & (temp_df['quantity'] > 0)]

            # Применяем метод FIFO
            quantity_to_sell = -row['quantity']
            profit = 0
            for purchase_index, purchase_row in previous_purchases.iterrows():
                if purchase_row['quantity'] <= quantity_to_sell:
                    profit += purchase_row['quantity'] * (row['price'] - purchase_row['price'])
                    quantity_to_sell -= purchase_row['quantity']
                    temp_df.loc[purchase_index, 'quantity'] = 0
                else:
                    profit += quantity_to_sell * (row['price'] - purchase_row['price'])
                    temp_df.loc[purchase_index, 'quantity'] -= quantity_to_sell
                    quantity_to_sell = 0
                if quantity_to_sell == 0:
                    break

            # Добавляем информацию о прибыли в profit_list
            profit_list.append(
                {'date_oper': row['date_oper'], 'client': row['client'], 'share': row['share'], 'quantity': row['quantity'], 'profit': profit})

connection.commit()
connection.close()
# Преобразуем profit_list в DataFrame и сохраняем в Excel
profit_df = pd.DataFrame(profit_list)
profit_df.to_excel("profit.xlsx", index=False)
print(profit_df)