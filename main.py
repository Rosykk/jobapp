import converter
import database

if __name__ == '__main__':
    urls = [
        {
            'url': 'https://urlhaus.abuse.ch/downloads/csv_recent/', 'delimiter': ',', 'column': 2
        },
        {
            'url': 'https://reputation.alienvault.com/reputation.data', 'delimiter': '#', 'column': 0
        },
        {
            'url': 'https://openphish.com/feed.txt', 'delimiter': '\n', 'column': 0
        }
    ]

    db = database.Database()

    try:
        db.connect()
        c = converter.Converter(db)
        c.fetch_urls(urls)
    except Exception as err:
        print(err)
    finally:
        db.disconnect()
