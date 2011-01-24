#!/usr/bin/env python2.6
# meta_tables.py
# written by jared kofron <jared.kofron@gmail.com>
# This python script handles the nastiness of writing
# the file that creates all of the necessary data tables
# for the slow control persistence database.
# DISCLAIMER: i am not terribly good at python
import os, sys
from getopt import gnu_getopt as getopt
from calendar import isleap

# pad a too-small value with zeroes to a given field width
def zeropad(s, w=2):
    zeroes = ''.join(['0' for x in range(w - len(s))])
    return zeroes + s

# often used pattern, here's a utility function
def ints_to_strings(ints):
    strings = [s for (s,w) in [(str(x),w) for (x,w) in ints]]
    return tuple(strings)

# generate an sql string that creates a monthly table.
def emit_monthly_table(year, month):
    (yearstr, monthstr) = ints_to_strings([(year,4),(month,2)])
    yr_const = "(extract(year from ts)::int = {0})".format(yearstr)
    mo_const = "(extract(month from ts)::int = {0})".format(monthstr)
    ov_const = "check({0} and {1})".format(yr_const,mo_const)
    res = "create table y{0}m{1} ({2}) inherits ({3});"
    return res.format(yearstr,monthstr,ov_const,master_name())

# emit an sql string that creates a daily summary table.  these tables
# are partitions on the dailyAvg table.
def emit_daily_avg_partition(year):
    (yearstr,) = ints_to_strings([(year,4)])
    yr_const = "check(extract(year from day)::int = {0})".format(yearstr)
    res = "create table y{0}avgDay ({1}) inherits ({2});"
    return res.format(yearstr,yr_const,day_master_name())

# generate an sql string that creates a weekly hourly summary
# table
def emit_weekly_hourly_table(year,day,hr):
    (yearstr, daystr, hrstr) = ints_to_strings([(year,4),(day,3),(hr,2)])
    res = "create table y{0}d{1}h{2}avgHr () inherits ({3});"
    return res.format(yearstr, daystr, hrstr,hour_master_name())

# generate the sql statement that will create the master table
def emit_master_table():
    f_meas_id = "meas_id int primary key default nextval('measurement_ids')"
    f_ts = "ts timestamp not null"
    f_card = "card varchar not null"
    f_channel = "channel integer not null"
    f_host = "hostname varchar not null"
    f_value = "value real not null"

    return "create table" +\
        " " +\
        master_name() +\
        " (" +\
        f_meas_id +\
        "," +\
        f_host +\
        "," +\
        f_card +\
        "," +\
        f_channel +\
        "," +\
        f_value +\
        "," +\
        f_ts +\
        ");"

# the name of the master table
def master_name():
    return "meas_master"

# the daily average table
def emit_daily_avg_table():
    f_rowid = "row_id int primary key default nextval('avg_ids')" 
    f_upcnt = "ucount int not null"
    f_host = "hostname varchar not null"
    f_card = "card varchar not null"
    f_channel = "channel integer not null"
    f_date = "day date not null"
    f_min  = "minval real not null"
    f_max  = "maxval real not null"
    f_avg  = "avgval real not null"
    
    return "create table" +\
        " " +\
        day_master_name() +\
        " (" +\
        f_rowid +\
        "," +\
        f_upcnt +\
        "," +\
        f_host +\
        "," +\
        f_card +\
        "," +\
        f_channel +\
        "," +\
        f_date +\
        "," +\
        f_min +\
        "," +\
        f_max +\
        "," +\
        f_avg +\
        ");"

# the name of the hourly master table
def hour_master_name():
    return "hourly_master"

# the name of the daily master table
def day_master_name():
    return "daily_master"

# main routine
def main(sysargs):
    year_begin = 2010
    year_end = 2015
    short_opts = "s:e:"
    long_opts = []
    try:
        (opts, args) = getopt(sysargs[1:], short_opts, long_opts)
    except Exception as err:
        print(str(err))
    for (o,a) in opts:
        if o in ("-s"):
            try:
                year_begin = int(a)
            except Exception as err:
                print("couldn't convert argument of 's' to integer.")
                exit(2)
        elif o in ("-e"):
            try:
                year_end = int(a)
            except Exception as err:
                print("couldn't convert argument of 'e' to integer.")
                exit(2)
    if year_begin > year_end:
        print("starting year must be before ending year.")
        exit(2)
    # open outfile
    outfile = open('create_tables.sql','w')
    
    # emit master tablename
    outfile.write(emit_master_table() + "\n")

    # emit daily master tablename
    outfile.write(emit_daily_avg_table() + "\n")

    for year in range(year_begin, year_end + 1):
        # emit daily avg partitions
        outfile.write(emit_daily_avg_partition(year) + "\n")
        # emit weekly tablenames
        for month in range(1,13):
            outfile.write(emit_monthly_table(year,month) + "\n")

    # done
    print("output written on create_tables.sql")
    exit(0)

# entry
if __name__ == '__main__':
    main(sys.argv)
