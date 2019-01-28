"""Summary

Attributes:
    pgrest (TYPE): Description
    waze_endpoint (TYPE): Description
"""
import pandas as pd
import requests
import json

from tdutils.pgrestutil import Postgrest
from pypgrest import Postgrest

from config.secrets import *

waze_endpoint = waze_endpoint


pgrest = Postgrest(
    "http://transportation-data-test.austintexas.io/waze_archive",
    auth=JOB_DB_API_TOKEN_test,
)


def scrape_waze_record():
    """Summary
    
    Returns:
        TYPE: Description
    """
    # download json

    r = requests.get(waze_endpoint)
    alerts_list_raw = r.json()["alerts"]
    # pd.DataFrame.from_dict(alerts_list[0])
    return alerts_list_raw


def split_coord(alerts_list_raw):
    """Summary
    
    Args:
        alerts_list_raw (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    alerts_list = alerts_list_raw

    for alert in alerts_list:
        alert["longitude"] = alert["location"]["x"]
        alert["latitude"] = alert["location"]["y"]
        alert.pop("location", None)

    return alerts_list


def prepare_payloads(alerts_list):
    """Summary
    
    Args:
        alerts_list (TYPE): Description
    """
    alerts_df = pd.DataFrame.from_dict(alerts_list)


def post_to_postgre(alerts_list):
    """Summary
    
    Args:
        alerts_list (TYPE): Description
    
    Returns:
        TYPE: Description
    """

    counter = 0

    for index, alert in enumerate(alerts_list):
        alerts_list[index] = dict((k.lower(), v) for k, v in alert.items())

    for alert in alerts_list:

        try:
            pgrest.upsert(alert)
            counter+=1


        except:
            return pgrest.res.text

    return alerts_list

def main():
    """Summary
    
    Returns:
        TYPE: Description
    """
    alerts_list_raw = scrape_waze_record()
    alerts_list = split_coord(alerts_list_raw)

    res = post_to_postgre(alerts_list)

    return len(res)


if __name__ == "__main__":

    print(main())

