import datetime
import os
import time

import requests
import scraperwiki

days_offset_from = int(os.getenv('MORPH_DAYS_OFFSET_FROM', 1))
days_offset_to = int(os.getenv('MORPH_DAYS_OFFSET_TO', 0))
progress = os.getenv('MORPH_PROGRESS', 'all')  # In Progress|Decided|Past|all
councils = os.getenv('MORPH_COUNCILS', 'ipswich,sunshinecoast').split(",")


today = datetime.datetime.strftime(datetime.datetime.now(), "%m-%d-%Y")

urls = dict(
    ipswich=dict(
        url='https://developmenti.ipswich.qld.gov.au/Geo/GetApplicationFilterResults',
        info_url='https://developmenti.ipswich.qld.gov.au/Home/FilterDirect?filters=DANumber=',
        property_details_url='https://developmenti.ipswich.qld.gov.au/Geo/GetPropertyDetailsByLandNumber?landNumber=',
    ),
    brisbane=dict(
        url='https://developmenti.brisbane.qld.gov.au/Geo/GetApplicationFilterResults',
        info_url='https://developmenti.brisbane.qld.gov.au/Home/FilterDirect?filters=DANumber=',
        property_details_url='https://developmenti.brisbane.qld.gov.au/Geo/GetPropertyDetailsByLandNumber?landNumber=',
    ),
    sunshinecoast=dict(
        url='https://developmenti.sunshinecoast.qld.gov.au/Geo/GetApplicationFilterResults',
        info_url='https://developmenti.sunshinecoast.qld.gov.au/Home/FilterDirect?filters=DANumber=',
        property_details_url='https://developmenti.sunshinecoast.qld.gov.au/Geo/GetPropertyDetailsByLandNumber?landNumber=',
    ),
)

# there seem to be a lot of duplicates
council_references = set()


def extract_feature(feature, council):
    lng, lat = feature['geometry']['coordinates']
    properties = feature['properties']
    council_reference = properties['application_number']
    try:
        record = scraperwiki.sql.select("* from data where authority_label=? and council_reference=?", [council, council_reference])[0]
    except:
        record = dict()
    record.update(
        council_reference=council_reference,
        authority_label=council,
        description=properties['description'],
        category_desc=properties['category_desc'],
        info_url=urls[council]['info_url'] + properties['application_number'],
        date_received=properties['date_received'],
        progress=properties['progress'],
        date_scraped=today,
        lat=lat,
        lng=lng,
        land_id=properties.get('land_no')
    )
    if council_reference not in council_references:
        council_references.add(council_reference)
        print("Saving %s" % council_reference)
        scraperwiki.sqlite.save(['authority_label', 'council_reference'], record)


for council in councils:
    print("Importing development.i records for %s" % council)
    has_more_pages = True
    total_number_returned = 0

    while has_more_pages:
        print("Downloading Applications with offset %d" % total_number_returned)
        number_returned = 0
        resp = requests.post(urls[council]['url'], json={
            "Progress": "all",
            "StartDateUnixEpochNumber": int(str(int(time.mktime((datetime.date.today() - datetime.timedelta(days=days_offset_from)).timetuple()))) + "000"),
            "EndDateUnixEpochNumber": int(str(int(time.mktime((datetime.date.today() - datetime.timedelta(days=days_offset_to) + datetime.timedelta(days=1)).timetuple()))) + "999"),
            "DateRangeField": "submitted",
            "SortField": "submitted",
            "SortAscending": False,
            "PagingStartIndex": total_number_returned,
            "MaxRecords": 200,
            "ShowCode": True, "ShowImpact": True, "ShowOther": True, "ShowIAGA": True, "ShowIAGI": True,
            "ShowRequest": True,
            "ShowNotifiableCode": True,
            "ShowReferralResponse": True,
            "IncludeAroundMe": False,
            "PixelWidth": 800, "PixelHeight": 800
        })

        raw = resp.json()

        for feature in raw['features']:
            extract_feature(feature, council)
            number_returned += 1

        for multiSpot in raw['multiSpot'].values():
            for feature in multiSpot:
                extract_feature(feature, council)
                number_returned += 1

        number_returned = raw.get('numberReturned', number_returned)  # not all support numberReturned
        total_number_returned += number_returned
        total_features = raw['totalFeatures']
        has_more_pages = total_number_returned < total_features

    # populate each DA's address and lot plan if missing
    missing_address_query = "* from data where authority_label=? and land_id is not null"
    if 'address' in list(scraperwiki.sql.dt.column_names('data')):
        missing_address_query += " and (address is null or lot_plan is null)"
    das = scraperwiki.sql.select(missing_address_query, [council])

    print("Populate %d DA's with address and lot/plan" % len(das))
    for da in das:
        resp = requests.get(urls[council]['property_details_url'] + da['land_id'])
        if resp.ok:
            properties = resp.json()['features'][0]['properties']
            da['address'] = properties['address_format']
            da['lot_plan'] = properties['lot_plan']
            print("Updating %s -> %s" % (da['council_reference'], da['address']))
            scraperwiki.sqlite.save(['authority_label', 'council_reference'], da)
