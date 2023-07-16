
--Field Finder
select distinct * from information_schema.columns c 
where column_name like '%posnr%' 
order by table_name


--Lineside Vehicle Location
with track as 
(select distinct row_number()over(partition by serial_number_s order by last_modified_time desc)as row_id, serial_number_s,station_name_s from raw_data_mes.at_station_operation)
select 
uda.sales_order_number_s "Sales Order",
uda.sequence_number_s "Sequence No",
serial_number_s "VIN Number",
station_name_s  "Station"
from track 
left join (select distinct * from raw_data_mes.uda_order where last_modified_time >= dateadd (hour ,-12,getdate())) uda on track.serial_number_s = uda.serial_no_s
			where row_id in (select min(row_id)
					         from track group by serial_number_s ,station_name_s,row_id
						     having track.row_id = 1)
			and uda.sequence_number_s not like ''
			and uda.serial_no_s not like ''
			and uda.serial_no_s not like 'M11%'
order by uda.sequence_number_s desc

