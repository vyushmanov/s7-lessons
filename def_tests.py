from datetime import datetime, timedelta

date = datetime.strptime('2023-06-06', '%Y-%m-%d')+ timedelta(days=-2)
print(date)

for i in range(3):
    print('Шаг цикла', i)

a = '5'
print(int(a))