import subprocess

def urls():
    urldaily = "http://datapoint.metoffice.gov.uk/public/data/val/wxfcs/all/json/all?res=daily&key={}"
    urlhourly = "http://datapoint.metoffice.gov.uk/public/data/val/wxobs/all/json/all?res=hourly&key={}"
    return urldaily, urlhourly


def dailydatacolumnsmapping():
    return {
        'i': 'siteid',
        'lat': 'latitude',
        'Dm': 'day_max_temperature',
        'Period.value': 'weather_date',
        'FDm': 'feels_like_day_max_temperature',
        'Gn': 'wind_gust_noon',
        'U': 'max_uv_index',
        'name': 'region',
        'Hn': 'screen_relative_humidity_noon',
        'PPd': 'precipitation_probability_day',
        'lon': 'longitude',
        'D': 'wind_direction',
        'Gm': 'wind_gust_midnight',
        'Hm': 'screen_relative_humidity_midnight',
        'PPn': 'precipitation_probability_night',
        'S': 'wind_speed',
        'V': 'visibility',
        'Nm': 'night_min_temperature',
        'FNm': 'feels_like_night_min_temperature',
        'W': 'weather_type',
        'max_uv_index': '0',
        '$': 'daynight_indicator',
    }


def dailydatacolconvert():
    return ['day_max_temperature',
            'feels_like_day_max_temperature',
            'feels_like_night_min_temperature',
            'wind_gust_midnight',
            'wind_gust_noon',
            'screen_relative_humidity_midnight',
            'screen_relative_humidity_noon',
            'night_min_temperature',
            'precipitation_probability_day',
            'wind_speed',
            'max_uv_index',
            'weather_type',
            'longitude',
            'latitude',
            'siteid',
            'precipitation_probability_night']


def hourlydatacolumnmapping():
    return {
        'i': 'siteid',
        'lat': 'latitude',
        'G': 'wind_gust',
        'Period.value': 'weather_date',
        'T': 'temperature',
        'name': 'region',
        'H': 'screen_relative_humidity',
        'P': 'pressure',
        'lon': 'longitude',
        'D': 'wind_direction',
        'value': 'weather_date',
        'Pt': 'pressure_tendency',
        'S': 'wind_speed',
        'V': 'visibility',
        'W': 'weather_type',
        'Dp': 'dew_point',
        '$': 'minutes'
    }


def hourlydatacolconvert():
    return ['minutes',
            'pressure',
            'wind_speed',
            'visibility',
            'weather_type',
            'siteid',
            'dew_point',
            'screen_relative_humidity',
            'temperature',
            'latitude',
            'longitude',
            'wind_gust']


def addnewcolumns():
    return ['dl_filename',
            'dl_line_no',
            'dl_file_date',
            'dl_insert_dttm',
            'dl_talend_job_id',
            'dl_talend_job_run_id']


def locationkeys():
    return ["name", "lat", "lon", "i", ["Period", "value"]]


def run_cmd(args_list):
    print('Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return = proc.returncode
    return s_return, s_output, s_err


def renamefile(temppath, tempfile):
    run_cmd(['hadoop', 'fs', '-put', '-f' temppath, tempfile])


def deletetemphdfsfolder(tempfolder):
    run_cmd(['hadoop', 'fs', '-rm', '-r', tempfolder])
