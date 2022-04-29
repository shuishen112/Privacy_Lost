from archived.utils import get_historical_url


def test_get_historical_url():
    url = "southern.edu"
    type = "edu"
    year = 2014

    historical_url = get_historical_url(type, url, year)
    assert (
        historical_url
        == "https://web.archive.org/web/20140412112630/http://southern.edu/"
    )

    url = "smugmug.com"
    type = "sample"
    year = 2017
    historical_url = get_historical_url(type, url, year)
    assert (
        historical_url
        == "https://web.archive.org/web/20170409214644/https://www.smugmug.com/"
    )
