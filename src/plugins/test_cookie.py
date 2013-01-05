import httputils

c=httputils.HTTPClient()
pg=c.load_page('http://localhost/trance')
print(pg)
pg=c.load_page('http://localhost/trance')
print(pg.find('p', id='cookie'))
