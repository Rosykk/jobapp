import re
import requests


class Converter:
    def __init__(self, db):
        self.db = db

    def data_source(self, url):
        """Insert data source into the database."""
        website = re.search(r'(https?://)(.*?)(?=/)', url)

        if website:
            website = website.group(2)

        # if data source does not exist, insert it into the database
        check = 'SELECT id FROM data_sources WHERE name = %s;'
        query = """
            INSERT INTO data_sources (name)
            VALUES (%s)
            RETURNING data_sources.id;
        """

        record_id = self.db.execute_query(check, (website,))
        if not record_id:
            self.db.execute_query(query, (website,))
            print(f'[Convertor] inserted {website} into data_sources table, skipping.')

            # get id of inserted record
            record_id = self.db.execute_query(check, (website,))
        else:
            print(f'[Convertor] {website} already exists in data_sources table')

        return record_id

    def fetch_urls(self, url_list):
        """Fetch urls from the internet and insert them into the database."""

        for url in url_list:
            try:
                response = requests.get(url['url'])
            except requests.exceptions.RequestException as e:
                print('[Convertor] error while fetching url:', e)
                continue

            data = response.text
            rows = data.split('\n')

            # get data source id
            data_source_id = self.data_source(url['url'])

            for row in rows:

                if row.startswith('#'):
                    continue

                values = row.split(url['delimiter'])

                if len(values) < url['column']:
                    continue

                # insert columns into database
                self.match_data(values[url['column']], data_source_id)

    def match_data(self, value, data_source_id):
        """Match the given value to a regex and insert it into the database."""
        if re.search(r'https?://.*', value):
            self.insert_url(value, data_source_id)

        elif re.search(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$', value):
            self.insert_ip_address(value, data_source_id)

        else:
            print(f'[Convertor] "{value}" is not a valid URL or IP address')

    def insert_ip_address(self, ip_address, data_source_id):
        """Insert the given IP address into the database."""
        insert = """
            INSERT INTO ip_addresses (ip_address, data_source)
            VALUES (%s, %s)
            RETURNING id;
        """
        if self.db.execute_query(insert, (ip_address, data_source_id)):
            self.db.commit()

    def insert_url(self, url, data_source_id):
        """Insert the given URL into the database."""
        insert = """
            INSERT INTO urls (url, data_source)
            VALUES (%s, %s)
            RETURNING id;
        """
        if self.db.execute_query(insert, (url, data_source_id)):
            self.db.commit()
