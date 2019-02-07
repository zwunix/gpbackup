CREATE TABLE sales (
    id integer,
    date date,
    amt numeric(10,2)
) DISTRIBUTED BY (id) PARTITION BY RANGE(date)
          (
          PARTITION jan17 START ('2017-01-01'::date) END ('2017-02-01'::date) WITH (tablename='sales_1_prt_jan17', appendonly=false ),
          PARTITION feb17 START ('2017-02-01'::date) END ('2017-03-01'::date) WITH (tablename='sales_1_prt_feb17', appendonly=false ),
          PARTITION mar17 START ('2017-03-01'::date) END ('2017-04-01'::date) WITH (tablename='sales_1_prt_mar17', appendonly=false ),
          PARTITION apr17 START ('2017-04-01'::date) END ('2017-05-01'::date) WITH (tablename='sales_1_prt_apr17', appendonly=false ),
          PARTITION may17 START ('2017-05-01'::date) END ('2017-06-01'::date) WITH (tablename='sales_1_prt_may17', appendonly=false ),
          PARTITION jun17 START ('2017-06-01'::date) END ('2017-07-01'::date) WITH (tablename='sales_1_prt_jun17', appendonly=false ),
          PARTITION jul17 START ('2017-07-01'::date) END ('2017-08-01'::date) WITH (tablename='sales_1_prt_jul17', appendonly=false ),
          PARTITION aug17 START ('2017-08-01'::date) END ('2017-09-01'::date) WITH (tablename='sales_1_prt_aug17', appendonly=false ),
          PARTITION sep17 START ('2017-09-01'::date) END ('2017-10-01'::date) WITH (tablename='sales_1_prt_sep17', appendonly=false ),
          PARTITION oct17 START ('2017-10-01'::date) END ('2017-11-01'::date) WITH (tablename='sales_1_prt_oct17', appendonly=false ),
          PARTITION nov17 START ('2017-11-01'::date) END ('2017-12-01'::date) WITH (tablename='sales_1_prt_nov17', appendonly=false ),
          PARTITION dec17 START ('2017-12-01'::date) END ('2018-01-01'::date) WITH (tablename='sales_1_prt_dec17', appendonly=false )
          );

INSERT into sales VALUES(19, '2017-02-15'::date, 100);

CREATE TABLE foo (i int);
INSERT INTO foo VALUES (1);
