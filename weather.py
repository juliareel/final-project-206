def create_request_url(city, date1, date2):
    base_url = 'https://api.weatherstack.com/historical?access_key=c1e596fa44c50dbf9597860a4e84a937'
    params = '&query='+ city + '&historical_date_start=' + date1 + '&historical_date_end=' + date2
    url = base_url + params
    return url

