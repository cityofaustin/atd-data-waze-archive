import pandas as pd
import requests
import json

waze_endpoint = "https://na-georss.waze.com/rtserver/web/TGeoRSS?tk=ccp_partner&ccp_partner_name=City%20of%20Austin&format=JSON&types=traffic,alerts,irregularities&polygon=-97.935000,30.189000;-97.857000,30.141000;-97.826000,30.150000;-97.786000,30.143000;-97.718000,30.163000;-97.704000,30.193000;-97.646000,30.149000;-97.624000,30.141000;-97.599000,30.167000;-97.641000,30.182000;-97.627000,30.226000;-97.575000,30.306000;-97.652000,30.352000;-97.656000,30.473000;-97.733000,30.472000;-97.749000,30.526000;-97.822000,30.496000;-97.920000,30.390000;-97.942000,30.325000;-97.935000,30.189000;-97.935000,30.189000"

def scrape_waze_record():

    # download json
    r = requests.get(waze_endpoint)
    alerts_list_raw = r.json()["alerts"]
    # pd.DataFrame.from_dict(alerts_list[0])
    return alerts_list_raw

def split_coord(alerts_list_raw):

    for alert in alerts_list_raw:
        alert["longitude"] = alert["location"]["x"]
        alert["latitude"] = alert["location"]["y"]
        alert.pop('location', None)

    return alerts_list_raw

def main():
    alerts_list_raw = scrape_waze_record()
    alert_list = split_coord(alerts_list_raw)


    return alert_list

if __name__ == "__main__":
    print(main())


# columns = []
# for key, value in alerts_list[0].items() :
#     columns.append(key)

# for alert in alerts_list:
#     new_row = pd.DataFrame([alert])
#     alerts_df.append(new_row, sort=False)
#     print(new_row)
#     print(alerts_df)
#     break