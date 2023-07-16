
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


--COOIS Replica w/VIN
select distinct 
resb.rsnum "Reservation #",
afpo_plaf.kdauf "Sales Order",
afpo_plaf.plnum "Planned Order",
afpo_plaf.aufnr "Production Order",
resb.matnr "Material",
resb.bdter::date "Requirements date",
resb.bdmng "Requirement Quantity",
resb.enmng "Quantity withdrawn",
resb.meins "Unit",
makt.maktx "Material Description",
afpo_plaf.charg "Batch",
null as "Sequence", -- PLNFL is the field but not in RESB anymore?
--afru.vornr "Avtivity", --Not needed as of now
resb.rspos "Item No",
resb.werks "Plant",
resb.lgort "Storage Location",
status.txt04 "Status",
zvlc_vlc.seq_no "Seq No",
zvlc_vlc.vhcle "Vehicle No",
zvlc_vlc.vhvin
from (select distinct rsnum,kdauf,plnum, null as aufnr ,null as charg from raw_data_sap.plaf 
			union 
	  select distinct null as rsnum,kdauf,plnum,aufnr,charg from raw_data_sap.afpo ) afpo_plaf
left join (select distinct aufpl,aufnr,rsnum from raw_data_sap.afko) afko on afko.aufnr = afpo_plaf.aufnr
left join (select distinct matnr,bdter,meins,rsnum,bdmng,enmng,aufnr,lgort,werks,rspos from raw_data_sap.resb) resb on resb.rsnum = afpo_plaf.rsnum or resb.rsnum = afko.rsnum
left join (select distinct matnr, maktx from raw_data_sap.makt ) makt on makt.matnr = resb.matnr
left join (select distinct afvc.aufpl,jest.objnr, t.txt04 from raw_data_sap.afvc afvc
				left join raw_data_sap.jest jest on jest.objnr = afvc.objnr 
				inner join raw_data_sap.tj02t t on jest.stat = t.istat
				where spras like 'E'
				and t.txt04 ='REL') status on status.aufpl = afko.aufpl 
left join ((select distinct vguid,aufnr,seq_no from raw_data_sap.zvlcprodord) zvlc 
			inner join (select distinct vguid, vhcle, vhvin from raw_data_sap.vlcvehicle) vlc on vlc.vguid = zvlc.vguid) zvlc_vlc on zvlc_vlc.aufnr = afpo_plaf.aufnr --or join on batch number?
where "Sales Order" like '%30028893'
order by afpo_plaf.aufnr,afpo_plaf.plnum,resb.rspos


--Upcoming Sales Orders
select distinct 
apt.kdauf,
apt.status,
case when apt.seq_no like '' then null else apt.seq_no end,
apt.vhvin
from manufacturing_datamart.amp1_production_tracker apt
where apt.status like 'Build Queue' 
order by seq_no 


--Upcoming Sales Order V2
with zvlcprodord as
(select distinct zvlc.vguid, zvlc.aufnr, zvlc.seq_no from raw_data_sap.zvlcprodord zvlc ),
vlcvehicle as
(select distinct vlc.vhvin, vlc.vguid , vlc.mmtsp from raw_data_sap.vlcvehicle vlc ),
zvlc_vlc as
(select zvlc.aufnr, zvlc.vguid, zvlc.seq_no, vlc.vhvin ,vlc.mmtsp from zvlcprodord zvlc inner join vlcvehicle vlc on vlc.vguid = zvlc.vguid ),
afpo as
(select distinct aufnr, matnr from raw_data_sap.afpo where matnr = 'M11-004236-01'),
afru as
(select max(budat::date) as budat_date, 
aufnr, 
max(CASE WHEN regexp_instr(vornr, '[^[:print:][:cntrl:]]') > 0 or vornr is null or vornr = '' 
THEN '0'ELSE vornr end::integer) AS vornr_max  
from raw_data_sap.afru afru
				where gmnga = 1 
				and stzhl <> '00000001' 
				--and afru.stzhl = ''
				and stokz <> 'X'
				and aueru = 'X'
				group by aufnr),
afru_filtered as
(select * from afru where vornr_max < 361),
	-- any record greater than 360 is not confirmed.
	-- any activity/operation which is greater than 360 and confirmed should be dropped
afko as
(select distinct aufnr, rsnum from raw_data_sap.afko),
aufk as
(select distinct aufk.aufnr, aufk.kdauf, aufk.erdat::date as erdat_date from raw_data_sap.aufk aufk),
aufk_jest as
(select aufk.aufnr, aufk.kdauf, jest.stat, jest.inact, aufk.erdat::date as erdat_date from raw_data_sap.aufk aufk
		inner join raw_data_sap.jest jest on jest.objnr = aufk.objnr ),
aufk_in_build_status as
(select distinct kdauf, aufnr, stat, erdat_date from aufk_jest aaj
		where aaj.aufnr not in (select distinct aufnr from aufk_jest where stat in ('I0045', 'I0012', 'I0046', 'I0076'))),
in_build as
(select distinct aibs.kdauf, aibs.aufnr, aibs.erdat_date as erdat_date, 1 as parent_order_flag from aufk_in_build_status aibs
	inner join afpo afpo on afpo.aufnr = aibs.aufnr
	inner join afru_filtered afru_filtered on afru_filtered.aufnr = aibs.aufnr),
in_build_final as
(select distinct ib.kdauf, aufk.aufnr, afko.rsnum, aufk.erdat_date, ib2.parent_order_flag  from in_build ib
	left join aufk aufk on aufk.kdauf = ib.kdauf
	left join in_build ib2 on ib2.kdauf+ib2.aufnr = ib.kdauf+aufk.aufnr
	left join afko afko on afko.aufnr = aufk.aufnr),
--build-queue
-- build-queue
  --  JEST Table should have I0001 or I0002 
  -- I0001 creation 
  -- I0002 release
	-- INACT <> 'X'
-- should not have any of the following:
  --  I0009 partially confirm, 
  --  fully confirmed I0010
  -- I0012 - 
  -- I0045 - 
  -- I0046 - 
  -- I0076 
aufk_build_queue_status AS 
(SELECT DISTINCT kdauf
		,aufnr
		,erdat_date
		FROM aufk_jest aaj
	WHERE aaj.aufnr NOT IN (
			        SELECT DISTINCT aufnr        
			        FROM aufk_jest        
			        WHERE stat IN ('I0009','I0010','I0012','I0045','I0046','I0076'))         
			     AND aaj.aufnr IN (
			        SELECT DISTINCT aufnr        
			        FROM aufk_jest        
			        WHERE stat IN ('I0001','I0002')
				and lower(inact) <> 'x')),
build_queue as 
(SELECT distinct kdauf, abqs.aufnr, erdat_date, 1 as parent_order_flag FROM aufk_build_queue_status abqs
	inner join afpo afpo on afpo.aufnr = abqs.aufnr
	where abqs.kdauf <> ''),
in_build_build_queue_status as
(select 'In Build' as status, kdauf, aufnr, erdat_date, parent_order_flag from in_build
	union all
	select 'Build Queue' as status, kdauf, aufnr, erdat_date, parent_order_flag from build_queue),
in_build_build_queue_seq_vin as
(select status, zvlc_vlc.seq_no, zvlc_vlc.vhvin,zvlc_vlc.mmtsp, ibbq.kdauf, ibbq.aufnr, ibbq.erdat_date, ibbq.parent_order_flag from in_build_build_queue_status ibbq
	left join zvlc_vlc zvlc_vlc on zvlc_vlc.aufnr = ibbq.aufnr),
in_build_build_queue_seq_vin_final as
(select distinct ibbqs.status, ibbqs.seq_no, ibbqs.vhvin, ibbqs.kdauf, aufk.aufnr, afko.rsnum, aufk.erdat_date, ibbqs2.parent_order_flag,ibbqs.mmtsp  from in_build_build_queue_seq_vin ibbqs
	left join aufk aufk on aufk.kdauf = ibbqs.kdauf
	left join afko afko on afko.aufnr = aufk.aufnr
	left join in_build_build_queue_status ibbqs2 on ibbqs2.kdauf+ibbqs2.aufnr = ibbqs.kdauf+aufk.aufnr),
plaf as
(select distinct plaf.kdauf, plaf.plnum, plaf.rsnum from raw_data_sap.plaf),
ga_planned as 
(select distinct plaf.kdauf, plaf.plnum, plaf.rsnum, plaf.psttr::date as order_date, 1 as parent_order_flag
	from raw_data_sap.vbap vbap
	inner join raw_data_sap.plaf plaf on plaf.kdauf = vbap.vbeln and plaf.kdpos = vbap.posnr and plaf.matnr = vbap.matnr
	inner join raw_data_sap.vbkd vbkd on vbkd.vbeln = vbap.vbeln
	inner join raw_data_sap.vbak vbak on vbak.vbeln = vbap.vbeln
	where vbkd.ihrez_e <> '' and vbkd.ihrez_e is not null),
ga_planned_max as
(SELECT * FROM 
		(SELECT *, ROW_NUMBER()OVER(PARTITION by kdauf ORDER BY plnum DESC) as ROW_NUM
			FROM ga_planned) AS T
			WHERE ROW_NUM = 1),
ga_planned_final as
(select distinct 'GA Planned Production' as status, '' as seq_no, '' as vhvin, gp.kdauf, plaf.plnum, plaf.rsnum, gp.order_date, gp2.parent_order_flag  from ga_planned_max gp
	left join plaf plaf on plaf.kdauf = gp.kdauf
	left join ga_planned gp2 on gp2.kdauf+gp2.plnum = gp.kdauf+plaf.plnum),
resb as
(select kdauf, aufnr, rsnum, matnr, bdter::date from raw_data_sap.resb),
makt as
(select matnr, maktx from raw_data_sap.makt),
inbuild_buildqueue_ga_planned_final as
(select status, seq_no, vhvin, kdauf, aufnr, rsnum, erdat_date, parent_order_flag, mmtsp from in_build_build_queue_seq_vin_final
	union all
select status, seq_no, vhvin, kdauf, plnum, rsnum, order_date, parent_order_flag,null as mmtsp  from ga_planned_final),
amp1_production_tracker_final as
(select distinct 
isbgas.status, 
seq_no, 
vhvin, 
isbgas.kdauf, 
--isbgas.aufnr, 
--isbgas.rsnum, 
--resb.matnr, 
--makt.maktx, 
--isbgas.erdat_date, 
--resb.bdter, 
--parent_order_flag ,
case when vhvin like '' then null 
else left(to_timestamp(mmtsp,'YYYYMMDDHHMISS'),19) end as "Creation Time"
from inbuild_buildqueue_ga_planned_final isbgas
	left join resb resb on resb.rsnum = isbgas.rsnum
	left join makt makt on makt.matnr = resb.matnr)
select * from amp1_production_tracker_final
where-- bdter::date < sysdate + 14 and 
status in ('Build Queue','In Build') 
order by seq_no desc
--limit 50


--Upcoming Sales Order Components
select distinct 
apt.status,
apt.seq_no,
apt.vhvin "VIN",
apt.kdauf "Sales Order",
apt.aufnr "Production Order",
apt.matnr "Part Number",
apt.maktx "Part Description",
mfe.station_id "Station",
right(mfe.lineside__storage_location,9),
sum(apt.matnrcount)/ count(*) over (partition by apt.matnr)
--mfe.supply__method
from (select distinct apt.status,apt.seq_no,apt.vhvin,apt.kdauf,apt.aufnr,apt.matnr,apt.maktx,apt.rsnum,apt.bdter,count(matnr) as matnrcount
		from manufacturing_datamart.amp1_production_tracker apt group by 1,2,3,4,5,6,7,8,9) apt
inner join (select distinct * from manufacturing_datamart.mfe_ga__master_data mfe) mfe on mfe.part_base_10 = left(apt.matnr,10)
where apt.status = 'Build Queue' 
group by 1,2,3,4,5,6,7,8,9
order by right(mfe.lineside__storage_location,9)--apt.seq_no desc ,apt.kdauf-- ,apt.aufnr ,apt.matnrcount




select distinct left(partnumber,10),*
from manufacturing_datamart.mfe_ga__master_data
where station_id like 'TR00'
order by station_id 


select distinct *
from raw_data_mes.bom_part_list -- Parts/Lineside location in MES

select distinct * 
from raw_data_mes.at_work_instruction_config -- Parts/Lineside location in MES


select * , count(matnr)
from manufacturing_datamart.amp1_production_tracker apt 
where apt.kdauf like '%40000895' --and apt.matnr like 'P11-HT1170-01'
group by 1,2,3,4,5,6,7,8,9,10,11,12






