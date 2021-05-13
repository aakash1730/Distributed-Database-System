#
# Assignment5 Interface
# Name: Akash Patel
#

import re
from math import sqrt, radians, sin, cos, atan2, pow


def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    regex = '^' + cityToSearch + '$'
    query_result = collection.find({"city": re.compile(regex, re.IGNORECASE)})
    f = open(saveLocation1, 'w')
    for result in query_result:
        print(result.get("name").upper() + "$" + result.get("full_address").upper() + "$" + result.get("city").upper() + "$" + result.get("state").upper(), file=f)
    f.close()


def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    query_result = collection.find({"categories": {"$in": categoriesToSearch}})
    f = open(saveLocation2, 'w')
    R = 3959
    latitude1_in_radian = radians(float(myLocation[0]))
    for result in query_result:
        latitude2_in_radian = radians(float(result.get("latitude")))
        difference_in_latitude = radians(float(result.get("latitude")) - float(myLocation[0]))
        difference_in_longitude = radians(float(result.get("longitude")) - float(myLocation[1]))
        a = pow(sin(difference_in_latitude / 2), 2) + cos(latitude1_in_radian) * cos(latitude2_in_radian) * pow(sin(difference_in_longitude / 2), 2)
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        distance = R * c
        if distance < maxDistance:
            print(result.get("name").upper(), file=f)
    f.close()