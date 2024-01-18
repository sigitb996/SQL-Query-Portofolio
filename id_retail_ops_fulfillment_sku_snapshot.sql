
-- CREATE TABLE IF NOT EXISTS id_retail_ops_fulfillment_sku_snapshot
-- (   lazada_sku STRING
--     , fulfillment_sku STRING
--     , asc_sku_status STRING
--     , business_type_level2 STRING
--     , item_id STRING
--     , fulfillment_sku_status STRING
--     , sourcability_status STRING
--     , preferred_supplier_id STRING
--     , preffered_supplier_name STRING
--     , preferred_supplier_code STRING
--     , group_company_name STRING
--     , all_suppliers_code STRING
--     , product_type STRING
--     , temperature_type STRING
--     , serial_num_management STRING
--     , is_shelf_life_value STRING
--     , shelf_life_value DOUBLE
--     , reject_at_inbound_shelf_life DOUBLE
--     , alert_before_expiry_shelf_life DOUBLE
--     , offline_before_expiry_shelf_life DOUBLE
--     , package_height DOUBLE
--     , package_length DOUBLE
--     , package_width DOUBLE
--     , product_weight DOUBLE
--     , latest_po_supplier_code STRING
--     , latest_po_supplier_name STRING
--     , latest_po_moq STRING
--     , latest_po_supplier_box_qty STRING
--     , latest_po_create_date STRING
--     , current_cost DOUBLE
--     , normal_cost DOUBLE)
-- PARTITIONED BY (ds STRING )
-- LIFECYCLE 600;

--ADD COLUMNS
-- ALTER TABLE id_retail_ops_fulfillment_sku_snapshot
-- ADD COLUMNS (
-- 	vat DOUBLE
-- );

-- ALTER TABLE id_retail_ops_fulfillment_sku_snapshot
-- CHANGE vat vat DOUBLE AFTER contract_type ;

--====COMBINE ALL FULFILLMENT SKU RELATED DATA ====

INSERT OVERWRITE TABLE id_retail_ops_fulfillment_sku_snapshot
PARTITION(ds)
Select cb.lazada_sku
    , cb.fulfillment_sku
	, cb.asc_sku_status
	, cb.business_type_level2
	, cb.item_id
	, cb.fulfillment_sku_status
	, cb.sourcability_status
	, cb.preferred_supplier as preferred_supplier_id
	, vv.supplier_name preferred_supplier_name 
	, vv.supplier_code as preferred_supplier_code
	, contract_type
	, vat
	, d.group_company_name
	, cb.all_suppliers_id as all_suppliers_code
	, cb.product_type
	, cb.temperature_type
	, cb.serial_num_management
	, cb.is_shelf_life_value
	, cb.shelf_life_value
	, cb.reject_at_inbound_shelf_life
	, cb.alert_before_expiry_shelf_life
	, cb.offline_before_expiry_shelf_life
	, cast(d.package_height as DOUBLE) package_height
	, cast(d.package_length as DOUBLE) package_length
	, cast(d.package_width as DOUBLE) package_width
	, cast(d.product_weight as DOUBLE) as product_weight
	, v.supplier_code latest_po_supplier_code
	, v.supplier_name latest_po_supplier_name
	, cs.min_order_quantity latest_po_moq
	, cs.box_quantity latest_po_supplier_box_qty
	, d.po_create_date as latest_po_create_date
	-- , d.latest_inbound_date
	, coalesce(sp_price.special_purchase_price, cast(csraw.cost as DOUBLE)) as current_cost
	, cast(cs.cost as DOUBLE) as normal_cost
	, sp_price.special_purchase_price as special_cost
	, '${bizdate}' as ds

from (Select pk.lazada_sku
    , pk.fulfillment_sku
	, pk.is_active as asc_sku_status
	, pk.business_type_level2
	, ascp.*
from (
    SELECT lazada_sku
    , fulfillment_sku
	, is_active
	, business_type_level2
	, sc_item_id 
	, ds
	from dim_lzd_prd_sku_core_id
	---SKU core data refreshment usually done at 4AM. Assume we will already have latest data everyday by 5AM.
	WHERE venture = 'ID'
	and business_type_level2 = 'LazMall - Retail'
	and ds = '${bizdate}') pk 
left join (
	SELECT *
	from id_retail_ops_ascp_products_snapshot 
	--data refreshment on 10AM since source data ASCP finished usually around ~9.40AM. Will take max data at running schedule for now.
	Where ds = 
	(Select max(ds)
				from id_retail_ops_ascp_products_snapshot)
				) ascp 
on pk.sc_item_id = ascp.item_id) cb 


left join (
		Select supplier_id
		, supplier_code
		, supplier_name
		, supplier_name_en
		, company_name
		from s_vendor_base_info
		---Vendor base info data refreshment usually done at 9.40AM
		Where ds = 
		(Select max(ds)
				from s_vendor_base_info)
		and country_code = 'ID') vv
	on cb.preferred_supplier = vv.supplier_id


--====dabao data for fulfillment related are all the same. Cost here using weighted====--
--====however data stored are using lazada SKU/Platform SKU====--
left join (
		Select sku as lazada_sku
		, fulfillment_sku
		, latest_inbound_date 
		, po_create_date
		, supplier_name
		, virtual_bundle_cost
		, package_height
		, package_length
		, package_width
		, product_weight
		, business_type_level2
		, virtual_bundle_components
		, seller_sku
		, group_company_name
		, ds
		from lazada_retail.lzd_dabao_inv_di
		---Dabao inventory data refreshment usually done at 9.40AM. Dabao inventory data supposedly safe to use max data on the run timing. No frequent data changing
		Where ds = 
		(Select max(ds)
				from lazada_retail.lzd_dabao_inv_di) 
		and business_type_level2 = 'LazMall - Retail'
		and venture = 'ID'
		) d
	on cb.lazada_sku = d.lazada_sku

--====cost data based on fulfillment SKU. No cost data for VK====--
--====however data stored are using lazada SKU/Platform SKU====--
left join (SELECT lazada_sku
		, supplier_id
		, preferred_supplier_id
		, cost
		, current_cost
		, special_cost
		, min_order_quantity
		, box_quantity
		, ds
		, row_number() over(partition by lazada_sku
					order by last_modify_date desc, min_order_quantity desc) rn 
		from dim_lzd_ret_prd_sku_cost_id
		---no info on data refreshment schedule.
		Where ds = 
		(Select max(ds)
				from dim_lzd_ret_prd_sku_cost_id)
		and venture = 'ID') cs
	on cb.lazada_sku = cs.lazada_sku
	and cs.rn = 1

left join (
		Select supplier_id
		, supplier_code
		, supplier_name
		, supplier_name_en
		, company_name
		, gmt_create
		, row_number() over(partition by supplier_name order by gmt_create desc) rn
		from s_vendor_base_info
		---Vendor base info data refreshment usually done at 9.40AM. Vendor data supposedly safe to use max data on the run timing. No frequent data changing
		Where ds = 
		(Select max(ds)
				from s_vendor_base_info)
		and country_code = 'ID') v
	on cs.supplier_id = v.supplier_id
	and v.rn = 1

left join (Select raw.*
, CASE WHEN '${bizdate}' > substr(raw.nextPriceDate,1,8) 
THEN raw.nextPrice 
-- WHEN '${bizdate}' <= substr(sp_price.price_deadline_date,1,8)
-- THEN sp_price.special_purchase_price
ELSE raw.purchase_price END as cost

from
    (SELECT *
    , get_json_object(attribute,"$.nextPriceDate") nextPriceDate
    , get_json_object(attribute,"$.nextPrice")  nextPrice
    , ROW_NUMBER () OVER (PARTITION BY sku_code ORDER BY gmt_modified DESC) rnk
    FROM s_ascp_purchase_price_history_lazada price
    WHERE ds = 
	(Select max(ds) from s_ascp_purchase_price_history_lazada )
    AND price.group_id IN (81031)) raw ) csraw 
on cb.fulfillment_sku = csraw.sku_code
-- and cs.supplier_id = csraw.supplier_id 
and csraw.rnk = 1


--get valid special purchase price
left join 
    (SELECT sku_code, supplier_id, purchase_price, special_purchase_price, price_start_date, price_deadline_date
    , group_id
    , ROW_NUMBER () OVER (PARTITION BY sku_code ORDER BY gmt_modified desc, price_deadline_date DESC) rnk
    FROM s_ascp_purchase_price_history_lazada price
    WHERE ds = 
	(Select max(ds) from s_ascp_purchase_price_history_lazada )
    AND price.group_id IN (81031)
    and special_purchase_price is not null
	and ds >= to_char(price_start_date, 'yyyymmdd')
    and ds <= to_char(price_deadline_date, 'yyyymmdd')
    -- and '${bizdate}' <= substr(price_deadline_date,1,8) -- this to get only valid special price
    ) sp_price

on cb.fulfillment_sku = sp_price.sku_code
and sp_price.rnk = 1
