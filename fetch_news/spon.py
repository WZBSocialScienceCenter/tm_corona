from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup


ARCHIVE_URL_FORMAT = 'https://www.spiegel.de/nachrichtenarchiv/artikel-{:02d}.{:02d}.{}.html'
START_DATE = datetime(2019, 12, 15)
END_DATE = datetime.today()

duration = END_DATE - START_DATE

for day in range(duration.days):
    fetch_date = START_DATE + timedelta(days=day)
    archive_url = ARCHIVE_URL_FORMAT.format(fetch_date.day, fetch_date.month, fetch_date.year)

    resp = requests.get(archive_url)

    if resp.ok:
        soup = BeautifulSoup(resp.content, 'html.parser')
        container = soup.find_all('section', attrs={'data-area': 'article-teaser-list'})

        if len(container) == 1:
            headlines_container = container[0].select('article')
            for hcont in headlines_container:
                if any(len(hcont.find_all('span', attrs={'data-conditional-flag': k})) != 0
                       for k in ('gallery', 'video', 'audio', 'paid')):
                    continue

                url = hcont.select_one('h2 a').attrs.get('href', '')
                if url:
                    headline = hcont.select_one('h2').text.strip().split('\n')[0]
                    hfoot = hcont.select('footer span')
                    if len(hfoot) == 3:
                        #date_str = hfoot[0].text.strip()   # already got date
                        articlecateg = hfoot[2].text.strip()
                    else:
                        pass   # TODO
        else:
            pass  # TODO
    else:
        pass  # TODO
