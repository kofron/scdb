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
		return null;
	end
$$ language plpgsql;

create trigger master_insert
	before insert on meas_master
	for each row execute procedure ensure_table_routing();
			