import datetime
import time

FY = {2013: 52, 2014: 52, 2015: 52, 2016: 52, 2017: 52, 2018: 53, 2019: 52, 2020: 52, 2021: 52, 2022: 52, 2023: 52, 2024: 53}
startDay = datetime.date(2013, 1, 4)

def DateToFYFW(inputDate=None,datetimeobj=datetime.datetime.today(),fmt='%Y-%m-%d',micronendday=None):
    yearlist = sorted(list(FY.keys()))
    if inputDate:
        currentDay = datetime.datetime.strptime(inputDate, fmt).date()
    else:
        if micronendday and datetimeobj.hour>=19:
            currentDay = (datetimeobj+datetime.timedelta(hours=6)).date()
        else:
            currentDay=datetimeobj.date()

    deltaW = (currentDay - startDay).days // 7
    weeks = 0
    for year in yearlist:
        weeks += FY[year]
        if deltaW < weeks:
            break
    fw = str(FY[year] - (weeks - deltaW) + 1)
    if len(fw) == 1:
        fw = '0' + fw
    outputResult = "{}{}".format(str(year), fw)
    return outputResult


def FYFWto8Days(inputFYFW=DateToFYFW(), fmt='%Y/%m/%d'):
    yearlist = sorted(list(FY.keys()))
    intputYear = int(str(inputFYFW)[:4])
    inputWeek = int(str(inputFYFW)[-2:])
    weeks = 0
    outputDate = []
    for i in range(yearlist.index(intputYear)): weeks += FY[yearlist[i]]
    for j in range(8):
        outputDate.append((datetime.timedelta(days=(weeks + inputWeek - 1) * 7 + j - 1) + startDay).strftime('%Y-%m-%d'))
    return outputDate