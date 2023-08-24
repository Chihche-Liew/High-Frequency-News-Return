proc import datafile = 'taq_tables.csv'
	out = work.taq_tables
	dbms = csv
	replace;
	getnames = no;
run;

%macro loop_through_files;

	proc sql noprint;
        select distinct input(substr(string, 3), yymmdd8.) into :date_list separated by ' ' from taq_tables;
      quit;
	
	%let n_dates=%sysfunc(countw(&date_list));
	%let prev_year = .;

    %do i = 1 %to &n_dates;

	  %let date=%scan(&date_list, &i);
        %let year=%sysfunc(year(&date));
        %let filename=ct%sysfunc(putn(&date, yymmddn8.));
		
		/*cleaning the abnormal trading observations*/
		data taq;
			set &filename;
			format datetime datetime20.;
			datetime = dhms(date, 0, 0, time); 
			if price>0 and size>0;
			if corr in (0,1,2) and (cond in ('', ' ', '@', '*', 'E', 'F', '@E', '@F', '*E', '*F'));
		run;

		/*generating a subquery/subtable to avoid the nested aggregate functions*/
		proc sql;
			create table d as
			select symbol,
				   datetime,
				   sum(size) as sum_size
			from taq
			group by symbol, datetime
			order by symbol, datetime;

			create table taq_1 as
			select *
			from taq as x
			left join d as y
			on x.symbol = y.symbol and x.datetime = y.datetime;
		quit;

		/*aggregating the trading observations by symbol and time*/
		proc sql;
			create table taq_agg as
			select symbol,
				   datetime,
				   count(*) as trade,
				   sum(size) as volume,
				   sum(price*(size/sum_size)) as wvprice
			from taq_1
			group by symbol, datetime;
		quit;

		/*setting global timestamp variables*/
		%let start_time = '09:45:00't;
		%let end_time = '16:00:00't;
		%let interval = '00:15:00't;
		/*%let date = '03JAN2013'd; */

		/*generating timestamps with 15-minute intervals and 23:59:59*/
		data ret;
			start = dhms(&date, 0, 0, &start_time);
		  	end = dhms(&date, 0, 0, &end_time);
		  	format datetime datetime20.;
		  	do datetime = start to end by &interval;
		  	  output;
		  	end;
		  	datetime = dhms(&date, 23, 59, 59);
		  	output;
		  	keep datetime;
		run;

		/*generating symbol table and merging it with timestamps then ordering the datasets*/
		proc sql;
 		   	create table sym as
 		   	select distinct symbol
		    from taq_agg;

		    create table ret as
		    select *
		    from sym
		    cross join ret;

			create table taq_sorted as
		    select symbol,
				   datetime,
				   trade,
				   volume,
				   wvprice
		    from taq_agg
 		   	order by symbol, datetime;

			create table ret_sorted as
		    select symbol,
				   datetime format = datetime20.
		    from ret
		    order by symbol, datetime;
		quit;

		/*merging the symbol+datetime frame with taq data forward with the nearest observations*/
		proc sql;
			create table taq_&date as
		    select
        		a.symbol,
        		a.datetime,
				trade,
				volume,
				wvprice
    		from
        		ret_sorted as a
    		left join
        		taq_sorted as b
    		on
        		a.symbol = b.symbol and
        		a.datetime >= b.datetime
    		group by
        		a.symbol,
        		a.datetime
			having
				b.datetime = max(b.datetime);
		quit;
		
		/* concat data by year */
		data taq_&year;
            set taq_&year taq_&date;
        run;
		
		/* export the concatenated data */
		%if &year ne &prev_year and &prev_year ne . %then %do;
            proc export data=taq_&prev_year outfile="taq_&prev_year..csv" dbms=csv replace;
            run;
        %end;

		%let prev_year = &year;
    %end;
%mend;

%loop_through_files;
