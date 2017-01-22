# Скрипт для скачивания данных с finam.ru

Использование:

```
python3 finamdownloader.py -s <SYMBOL> -y <SYMBOL-FILE> -f <FROM> -t <TO> -o <FILENAME> -p <PERIOD> [-m <MARKET>] [-l]
```

где:

    <SYMBOL> - тикер, который нужно скачать, если указать '?', то будет выведен список всех тикеров
    
    <SYMBOL_FILE> - путь к файлу со списком всех тикеров, которые должны быть скачаны

    <FROM> - дата начала отрезка в формате YYYYMMDD

    <TO> - дата конца отрезка в формате YYYYMMDD
    
    <OUTPUT> - имя файла, в который будут записаны скачанные данные, если указать '!', то имя файла будет сформировано автоматически

    <PERIOD> - таймфрейм, должен быть одним из: tick, 1min, 5min, 10min, 15min, 30min, hour, daily, week, month
    
    <MARKET> - необязательный параметр, указывающий id раздела, из которого нужно скачивать данные. Если указать '?', будет выведен список всех разделов и из id.

    Ключ -l включает заполнение пустых периодов

