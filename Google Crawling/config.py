import random
# import dropbox

user_agent_list = [
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
    ]

for _ in user_agent_list: 
    #Pick a random user agent
    user_agent = random.choice(user_agent_list)

#Set the headers 
headers = {'User-Agent': user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            'Cache-Control': 'no-cache'
}

# dbx = dropbox.Dropbox('sl.BfETvybupeFZL7XDB3riszW1D7fXDg1Sa4N6W8JYkLEWENJp7rWzSNtnQhAxEWZrU2jUPpKiGP5Z_XRtsV0fbxGHIsGJ68TVu-EA7qNs9J0CTHgq1e7q-gxkaMDHrz_FGTOdwZPZ-Pc:EUR')
