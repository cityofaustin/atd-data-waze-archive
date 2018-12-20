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

waze_endpoint = "https://na-georss.waze.com/rtserver/web/TGeoRSS?tk=ccp_partner&ccp_partner_name=City%20of%20Austin&format=JSON&types=traffic,alerts,irregularities&polygon=-97.935000,30.189000;-97.857000,30.141000;-97.826000,30.150000;-97.786000,30.143000;-97.718000,30.163000;-97.704000,30.193000;-97.646000,30.149000;-97.624000,30.141000;-97.599000,30.167000;-97.641000,30.182000;-97.627000,30.226000;-97.575000,30.306000;-97.652000,30.352000;-97.656000,30.473000;-97.733000,30.472000;-97.749000,30.526000;-97.822000,30.496000;-97.920000,30.390000;-97.942000,30.325000;-97.935000,30.189000;-97.935000,30.189000"


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

