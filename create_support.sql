-- create_support.sql
-- written by jared kofron <jared.kofron@gmail.com>
-- This sql script does the work of creating all of the "support"
-- for the slow control persistence database.  Essentially it creates
-- a number of types and so on that are used by the database internals.

-- An enumerated type which lists possible card slots on an ioserver.
create type card_slot as enum('cardA','cardB','cardC','cardD');

-- A dedicated sequence generator for measurement ids
create sequence measurement_ids;