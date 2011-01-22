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
    strings = [zeropad(s,w) for (s,w) in [(str(x),w) for (x,w) in ints]]
    return tuple(strings)

# generate an sql string that creates a weekly table.
def emit_monthly_table(year, month):
    (yearstr, monthstr) = ints_to_strings([(year,4),(month,2)])
    yr_const = "(extract(year from ts)::int = {0})".format(yearstr)
    mo_const = "(extract(month from ts)::int = {0})".format(monthstr)
    ov_const = "check({0} and {1})".format(yr_const,mo_const)
    res = "create table y{0}m{1} ({2}) inherits ({3});"
    return res.format(yearstr,monthstr,ov_const,master_name())

# generate an sql string that creates a weekly daily summary
# table
def emit_weekly_daily_table(year,day):
    (yearstr, daystr) = ints_to_strings([(year,4),(day,3)])
    res = "create table y{0}d{1}avgDay () inherits ({2});"
    return res.format(yearstr,daystr,day_master_name())

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
    f_card = "card card_slot not null"
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

    for year in range(year_begin, year_end + 1):
    # emit weekly tablenames
        for month in range(1,13):
            outfile.write(emit_monthly_table(year,month) + "\n")
# entry
if __name__ == '__main__':
    main(sys.argv)
