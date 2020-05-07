drop table Color;
drop table Red;
drop table Green;
drop table Blue;

create table Color(red int,green int,blue int) 
row format delimited fields terminated by ',' stored as textfile;

load data local inpath '${hiveconf:P}' overwrite into table Color;

create table Red(key int, count int);
create table Green(key int,count int);
create table Blue(key int,count int);

insert overwrite table Red
select Color.red, count(1) from Color 
group by Color.red;

insert overwrite table Green
select Color.green,count(1) from Color
group by Color.green;

insert overwrite table Blue
select Color.blue,count(1) from Color    
group by Color.blue; 

select "1",key,count from Red 
order by key;
select "2",key,count from Green
order by key;
select "3",key,count from Blue
order by key;
