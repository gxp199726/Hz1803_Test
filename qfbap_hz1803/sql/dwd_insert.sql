--load data to dwd_user
insert into qfbap_dwd.dwd_user
select 
user_id,
user_name,
user_gender,
user_birthday,
user_age,
constellation,
province,
city,
city_level,
e_mail,
op_mail,
mobile,
num_seg_mobile,
op_mobile,
register_time,
login_ip,
login_source,
request_user,
total_score,
used_score,
is_blacklist,
is_married,
education,
monthly_income,
profession,
current_timestamp() dw_date
from qfbap_ods.ods_user;

--load data to dwd_user_extend
insert into qfbap_dwd.dwd_user_extend
select
user_id,
user_gender,
is_pregnant_woman,
is_have_children,
is_have_car,
phone_brand,
phone_brand_level,
phone_cnt,
change_phone_cnt,
is_maja,
majia_account_cnt,
loyal_model,
shopping_type_model,
weight,
height,
current_timestamp() dw_date
from qfbap_ods.ods_user_extend;

--load data to dwd_biz_trade
insert into qfbap_dwd.dwd_biz_trade partition(dt=${hivevar:date_dt})
select
trade_id,
order_id,
user_id,
amount,
trade_type,
trade_time,
current_timestamp() dw_date
from qfbap_ods.ods_biz_trade;

--load to data dwd_cart
insert into qfbap_dwd.dwd_cart partition(dt=${hivevar:date_dt})
select
cart_id,
session_id,
user_id,
goods_id,
goods_num,
add_time,
cancle_time,
sumbit_time,
create_date,
current_timestamp() dw_date
from qfbap_ods.ods_cart;

--load data to dwd_code_category
insert into qfbap_dwd.dwd_code_category
select
first_category_id,
first_category_name,
second_category_id,
second_catery_name,
third_category_id,
third_category_name,
category_id,
current_timestamp() dw_date
from qfbap_ods.ods_code_category;

--load data to dwd_order_delivery
insert into qfbap_dwd.dwd_order_delivery partition(dt=${hivevar:date_dt})
select
order_id,
order_no,
consignee,
area_id,
area_name,
address,
mobile,
phone,
coupon_id,
coupon_money,
carriage_money,
create_time,
update_time,
addr_id,
current_timestamp() dw_date
from qfbap_ods.ods_order_delivery;

--load data to dwd_order_item
insert into qfbap_dwd.dwd_order_item partition(dt=${hivevar:date_dt})
select
user_id,
order_id,
order_no,
goods_id,
goods_no,
goods_name,
goods_amount,
shop_id,
shop_name,
curr_price,
market_price,
discount,
cost_price,
first_cart,
first_cart_name,
second_cart,
second_cart_name,
third_cart,
third_cart_name,
goods_desc,
current_timestamp() dw_date
from qfbap_ods.ods_order_item;

--load data to dwd_us_order
insert into qfbap_dwd.dwd_us_order partition(dt=${hivevar:date_dt})
select
order_id,
order_no,
order_date,
user_id,
user_name,
order_money,
order_type,
order_status,
pay_status,
pay_type,
order_source,
update_time,
date(update_time)
from qfbap_ods.ods_us_order;

--load data to dwd_user_app_pv
insert into qfbap_dwd.dwd_user_app_pv partition(dt=${hivevar:date_dt})
select 
log_id,
user_id,
imei,
log_time,
hour(log_time) log_hour,
visit_os,
os_version,
app_name,
app_version,
device_token,
visit_ip,
province,
city,
current_timestamp() dw_date
from qfbap_ods.ods_user_app_click_log;

--load data to dwd_user_pc_pv
insert into qfbap_dwd.dwd_user_pc_pv partition(dt=${hivevar:date_dt})
select 
max(log_id),
user_id,
session_id,
cookie_id,
min(visit_time) in_time,
max(visit_time) out_time,
case when min(visit_time) = max(visit_time) then 3 else max(visit_time ) - min(visit_time) end stay_time,
count(1) pv,
visit_os,
browser_name,
visit_ip,
province,
city,
current_timestamp() dw_date
from qfbap_ods.ods_user_pc_click_log
group by
user_id,
session_id,
cookie_id,
visit_os,
browser_name,
visit_ip,
province,
city;

--load data to dwd_user_addr
insert into qfbap_dwd.dwd_user_addr
select
user_id,
order_addr,
user_order_flag,
addr_id,
arear_id,
current_timestamp() dw_date
from qfbap_ods.ods_user_addr;


