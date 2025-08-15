import requests


def get_total_count(url, start_date, end_date):
    query_count = f"""
    SELECT
    COUNT(*)
    WHERE (trip_start_timestamp >= "{start_date}" :: floating_timestamp)
    AND (trip_end_timestamp < "{end_date}" :: floating_timestamp)
    """

    params = {
        "$query": query_count,
    }

    r = requests.get(url, params=params)
    line_num = 0
    for line in r.iter_lines():
        if line_num == 0:
            line_num += 1
            continue
        line = line.decode()
        line = line.replace('"', "")

    return int(line)
