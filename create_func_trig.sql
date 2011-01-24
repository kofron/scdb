create or replace function ensure_table_routing()
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
       		year integer = extract(year from NEW.ts)::integer;
		tablename text = 'y' || year || 'avgDay';
       begin
		-- Strategy:
		-- Try to update a row.  If the row doesn't exist,
		-- we create it instead.
		select * into prow from daily_master dm where 
		       (dm.day = newday 
		       and
		       dm.hostname = NEW.hostname
		       and
		       dm.card = NEW.card
		       and 
		       dm.channel = NEW.channel);
		if not found then
	  	   execute 'insert into '
		   	|| tablename
			|| '(ucount,hostname,card,channel,'
			|| 'day,minval,maxval,avgval)'
			|| ' '
			|| 'values ('
			|| 1
			|| ','
			|| quote_literal(NEW.hostname)
			|| ','
			|| quote_literal(NEW.card)
			|| ','
			|| NEW.channel
			|| ','
			|| quote_literal(newday)
			|| ','
			|| NEW.value
			|| ','
			|| NEW.value
			|| ','
			|| NEW.value
			|| ')';
			return null;
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
			update daily_master 
			set
				ucount	  = newc,
				avgval    = newavg,
				minval	  = newmin,
				maxval	  = newmax
			where	  
				row_id = prow.row_id;
								
			return null;
		end if;
		
       end		    
$$ language plpgsql;

create trigger master_routing
	before insert on meas_master
	for each row execute procedure ensure_table_routing();

create trigger update_averages
       before insert on meas_master
       for each row execute procedure update_daily_avg();
