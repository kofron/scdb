-- master_table_routing (trigger)
-- fires before inserts are made to meas_master.  takes the new row
-- and instead of inserting the row into meas_master dynamically
-- determines the tablename based on the timestamp and inserts the
-- row there.  this operation executes in (near) constant time as the
-- only operations are the string ops and an insert.
create or replace function master_table_routing()
	returns trigger as $$
	declare
		year integer = extract(year from NEW.ts)::integer;
		month integer = extract(month from NEW.ts)::integer;
		tablename text = 'y' || year || 'm' || month;
	begin
		execute 'insert into '
			|| tablename
			|| '(hostname,card,channel,ts,value)'
			|| ' '
			|| 'values ('
			|| quote_literal(NEW.hostname)
			|| ','
			|| quote_literal(NEW.card)
			|| ','
			|| NEW.channel
			|| ','
			|| quote_literal(NEW.ts)
			|| ','
			|| NEW.value
			|| ')';
		return NEW;
	end
$$ language plpgsql;

-- daily_table_routing (trigger)
-- fires when inserts are made to daily_master.  performs the same task
-- as master_table_routing but returns NULL, as it is not a member of a
-- chain of triggers but rather the last action to be performed.  it is
-- only called during the updating of averages.
create or replace function daily_table_routing()
       returns trigger as $$
       declare
		year integer = extract(year from NEW.measdate)::integer;
		tablename text = 'y' || year || 'avgDay';
       begin	       
       		execute 'insert into'
			|| ' '
			|| tablename
			|| '(ucount, hostname, card, channel,'
			|| 'measdate, minval, maxval, avgval) '
			|| 'values ('
			|| NEW.ucount
			|| ','
			|| quote_literal(NEW.hostname)
			|| ','
			|| quote_literal(NEW.card)
			|| ','
			|| NEW.channel
			|| ','
			|| quote_literal(NEW.measdate)
			|| ','
			|| NEW.minval
			|| ','
			|| NEW.maxval
			|| ','
			|| NEW.avgval
			|| ')';
		return NULL;
       end
$$ language plpgsql;

-- hourly_table_routing (trigger)
-- fires when inserts are made to hourly_master.  performs essentially
-- the same task as master_table_routing but returns NULL, as it is not
-- a member of a chain of triggers.  should only be called during a table
-- 'flush' of the staging area.
create or replace function hourly_table_routing()
       returns trigger as $$
       declare
		year integer = extract(year from NEW.ts)::integer;
		month integer = extract(month from NEW.ts)::integer;
		tablename text = 'y' || year || 'm' || 'avgHour';
       begin	       
       		execute 'insert into'
			|| tablename
			|| 'values ('
			|| NEW.*
			|| ')';
		return NULL;
       end
$$ language plpgsql;

-- minute_table_routing (trigger)
-- fires when inserts are made to minute_master.  performs essentially
-- the same task as (hourly/master)_table_routing but returns NULL, as
-- it is not a member of a chain of triggers.  should only be called 
-- during a table 'flush' of the staging area.
create or replace function minute_table_routing()
       returns trigger as $$
       declare
		year integer = extract(year from NEW.ts)::integer;
		month integer = extract(month from NEW.ts)::integer;
		tablename text = 'y' || year || 'm' || 'avgMinute';
       begin	       
       		execute 'insert into'
			|| tablename
			|| 'values ('
			|| NEW.*
			|| ')';
		return NULL;
       end
$$ language plpgsql;

-- update_daily_avg (trigger)
-- scans the daily staging table for the correct row and recalculates
-- the rolling average for the day based on new values.  if the row does
-- not exist in the daily staging table, it is created.  at the end of
-- this trigger, a function is called which flushes old rows to the final
-- (much larger) table and deletes those rows from the staging area.
create or replace function update_daily_avg()
       returns trigger as $$
       declare
		prow daily_master%ROWTYPE;
		newday date = (NEW.ts)::date;
		newc real;
		newavg real;
		newmin real;
		newmax real;
		oldc int;
		oldavg real;
       begin
		-- Strategy:
		-- Try to update a row.  If the row doesn't exist,
		-- we create it instead.
		select * into prow from daily_avg_stage stage where 
		       (stage.measdate = newday 
		       and
		       stage.hostname = NEW.hostname
		       and
		       stage.card = NEW.card
		       and 
		       stage.channel = NEW.channel);
		if not found then
		   insert into 
		   	  daily_avg_stage
				(ucount,hostname,card,
			  	channel,measdate,minval,maxval,avgval) 
			  values
				(1,NEW.hostname,NEW.card,
				NEW.channel,newday,
				NEW.value,NEW.value,NEW.value);
		else
			oldc   := prow.ucount;
			oldavg := prow.avgval;
			newc   := prow.ucount + 1;
			newavg := ((oldc*oldavg)+NEW.value)/newc;
			newmin := prow.minval;
			newmax := prow.maxval;
			if NEW.value < newmin then
			   newmin := NEW.value;
			elsif NEW.value > newmax then
			   newmax := NEW.value;
			end if;
			update daily_avg_stage 
			set
				ucount	  = newc,
				avgval    = newavg,
				minval	  = newmin,
				maxval	  = newmax
			where	  
				row_id = prow.row_id;
		end if;
		perform flush_and_destroy_daily();	
		return NULL;
       end		    
$$ language plpgsql; 

-- update_hourly_avg (function)
-- hourly version of update_daily_avg
create or replace function update_hourly_avg()
       returns trigger as $$
       declare
		prow hourly_master%ROWTYPE;
		newday date = (NEW.ts)::date;
		newhr  int = extract(hour from NEW.ts);
		newc real;
		newavg real;
		newmin real;
		newmax real;
		oldc int;
		oldavg real;
       begin
		-- Strategy:
		-- Try to update a row.  If the row doesn't exist,
		-- we create it instead.
		select * into prow from hourly_avg_stage stage where 
		       (stage.measdate = newday 
		       and
		       stage.hostname = NEW.hostname
		       and
		       stage.card = NEW.card
		       and 
		       stage.channel = NEW.channel
			   and
			   stage.hr = newhr);
		if not found then
		   insert into 
		   	  hourly_avg_stage
				(ucount,hostname,card,hour,
			  	channel,measdate,minval,maxval,avgval) 
			  values
				(1,NEW.hostname,NEW.card,newhr
				NEW.channel,newday,
				NEW.value,NEW.value,NEW.value);
		else
			oldc   := prow.ucount;
			oldavg := prow.avgval;
			newc   := prow.ucount + 1;
			newavg := ((oldc*oldavg)+NEW.value)/newc;
			newmin := prow.minval;
			newmax := prow.maxval;
			if NEW.value < newmin then
			   newmin := NEW.value;
			elsif NEW.value > newmax then
			   newmax := NEW.value;
			end if;
			update hourly_avg_stage 
			set
				ucount	  = newc,
				avgval    = newavg,
				minval	  = newmin,
				maxval	  = newmax
			where	  
				row_id = prow.row_id;
		end if;
		perform flush_and_destroy_hourly();	
		return NEW;
       end		    
$$ language plpgsql;  

-- update_minute_avg (function)
-- minute version of update_daily_avg
create or replace function update_minute_avg()
       returns trigger as $$
       declare
		prow minute_master%ROWTYPE;
		newday date = (NEW.ts)::date;
		newhr  int = extract(hour from NEW.ts);
		newmint int = extract(minute from NEW.ts);
		newc real;
		newavg real;
		newmin real;
		newmax real;
		oldc int;
		oldavg real;
       begin
		-- Strategy:
		-- Try to update a row.  If the row doesn't exist,
		-- we create it instead.
		select * into prow from minute_avg_stage stage where 
		       (stage.measdate = newday 
		       and
		       stage.hostname = NEW.hostname
		       and
		       stage.card = NEW.card
		       and 
		       stage.channel = NEW.channel
			   and
			   stage.min = newmint
			   and
			   stage.hr = newhr);
		if not found then
		   insert into 
		   	  minute_avg_stage
				(ucount,hostname,card,hr,min,
			  	channel,measdate,minval,maxval,avgval) 
			  values
				(1,NEW.hostname,NEW.card,newhr,newmint,
				NEW.channel,newday,
				NEW.value,NEW.value,NEW.value);
		else
			oldc   := prow.ucount;
			oldavg := prow.avgval;
			newc   := prow.ucount + 1;
			newavg := ((oldc*oldavg)+NEW.value)/newc;
			newmin := prow.minval;
			newmax := prow.maxval;
			if NEW.value < newmin then
			   newmin := NEW.value;
			elsif NEW.value > newmax then
			   newmax := NEW.value;
			end if;
			update minute_avg_stage 
			set
				ucount	  = newc,
				avgval    = newavg,
				minval	  = newmin,
				maxval	  = newmax
			where	  
				row_id = prow.row_id;
		end if;
		perform flush_and_destroy_minute();	
		return NEW;
       end		    
$$ language plpgsql;


-- flush_and_destroy_x (functions)
-- these are functions that scans the x_avg_stage tables for stale
-- data (defined as older than 20 x-s), and flushes it to the long
-- term (much larger) average tables.  rows successfully committed
-- to the long term x average tables are deleted from the staging 
-- tables.

-- daily version
create or replace function flush_and_destroy_daily()
       returns integer as $$
       declare
	srow daily_master%rowtype;
       begin
		for srow in 
		    select * from daily_avg_stage s
  		    where extract(day from age(s.measdate)) < -20
		loop
		    begin  
		    	   delete from 
					daily_avg_stage
				  where
					row_id = srow.row_id; 
		    	   insert into daily_master values(srow.*);
		    end;	   
		end loop;
		return 0;
       end    
$$ language plpgsql;

-- hourly version
create or replace function flush_and_destroy_hourly()
       returns integer as $$
       declare
	srow hourly_master%rowtype;
       begin
		for srow in 
		    select * from hourly_avg_stage s
  		    where extract(hour from age(s.measdate)) < -20
		loop
		    begin  
		    	   delete from 
					hourly_avg_stage
				  where
					row_id = srow.row_id; 
		    	   insert into hourly_master values(srow.*);
		    end;	   
		end loop;
		return 0;
       end    
$$ language plpgsql;

-- minute version
create or replace function flush_and_destroy_minute()
       returns integer as $$
       declare
	srow minute_master%rowtype;
       begin
		for srow in 
		    select * from minute_avg_stage s
  		    where extract(minute from age(s.measdate)) < -20
		loop
		    begin  
		    	   delete from 
					minute_avg_stage
				  where
					row_id = srow.row_id; 
		    	   insert into minute_master values(srow.*);
		    end;	   
		end loop;
		return 0;
       end    
$$ language plpgsql;

--------------
-- triggers --
--------------
create trigger master_routing
	before insert on meas_master
	for each row execute procedure master_table_routing();

create trigger update_daily_averages
       before insert on meas_master
       for each row execute procedure update_daily_avg();

create trigger update_hourly_averages
       before insert on meas_master
       for each row execute procedure update_hourly_avg();

create trigger update_minute_averages
       before insert on meas_master
       for each row execute procedure update_minute_avg();

create trigger daily_routing
       before insert on daily_master
       for each row execute procedure daily_table_routing();
