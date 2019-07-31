create or replace package pkg_chnl_o2o_data is

  -- Author  : ZHANGHL1195
  -- Created : 2018/8/2 16:16:34
  -- Purpose : O2O薪酬方案
  -- 日志记录 : select * from log_prog_run_data a 
  --             where lower(a.pkg_name)='pkg_chnl_o2o_data' and to_char(start_date,'yyyymmdd')=to_char(sysdate,'yyyymmdd')
  --             order by start_date,end_date;
  
  
  /*计提条件  T0月为考核月,
if 用户在T0月满足:
  预存款达50元、状态正常、出账(剔赠)话费大于0元,非三无用户
  then T1月支付
    else if 用户在T0月满足：
       预存款未达到50元
         and 用户在T1月满足:
           预存款达50元、状态正常、出账话费大于0元,非三无用户
           then 
              T2月支付              
     else  不支付 
  */
  
  
  /*日志查询
  select * 
from log_prog_run_data a 
where lower(a.pkg_name)='pkg_chnl_o2o_data' 
and to_char(start_date,'yyyymmdd')>=to_char(sysdate-1,'yyyymmdd')
order by start_date,end_date;
  
  
  */
  
-----准备数据
     PROCEDURE safely_drop( in_name IN VARCHAR2 ) ; 
     procedure p_o2o_prepare_data(p_month in varchar2) ;
      
---- 社会渠道-O2O促销经理
  -- 直销：30元/户 * 当月产能
  --结果表：select * from tmp_o2o_cuxiao_xinchou_fengcun where jifen_month='';
  --结果明细表 : select * from tmp_o2o_cuxiao_fengcun where jifen_month='';
     procedure p_o2o_cuxiao_sh(p_month in varchar2);
     
---- 社会渠道-O2O促销督导员
 ---- 直销：4元/户 * 当月产能 ,话费分成6个月 *0.01 净出账
  ----结果表：select * from tmp_o2o_dudao_xinchou_fengcun where jifen_month='';
  ----话费分成结果明细表 : select * from tmp_o2o_dudao_hffc_fengcun where p_month='';
     procedure p_o2o_dudao_sh(p_month in varchar2); 
     
---- 社会渠道-O2O项目经理
  -- 直销：30元/户 * 当月产能 ，分销：15元/户 * 当月产能
  ----结果表：select * from tmp_o2o_xiangmu_xc_fengcun where jifen_month='';
  ----结果明细表 : select * from tmp_o2o_xiangmu_fengcun where jifen_month='';
     procedure p_o2o_xiangmu_sh(p_month in varchar2);   
     
----政企渠道-O2O地推经理
  -- 直销：30元/户 * 当月产能
  ----结果表：select * from tmp_o2o_ditui_xinchou_fengcun where jifen_month='';
  ----结果明细表 : select * from  tmp_o2o_ditui_fengcun where jifen_month='';
     procedure p_o2o_ditui_zq(p_month in varchar2);     
          
----政企渠道-O2O地推督导
 ---- 直销：4元/户 * 当月产能 ,话费分成6个月 *0.01 净出账
  ----结果表：select * from tmp_o2o_dt_xinchou_fengcun where jifen_month='';
  ----话费分成结果明细表 : select * from tmp_o2o_dt_hffc_fengcun where p_month='';
     procedure p_o2o_dudao_zq(p_month in varchar2); 
     
----政企渠道-O2O王卡兼职生
  ----结果表：select * from tmp_o2o_jz_xinchou_fengcun where jifen_month='';
  ----结果明细表 : select * from tmp_o2o_jianzhi_all where jifen_month='';
     procedure p_o2o_jianzhi_zq(p_month in varchar2);    
     
----营业政企校园家互渠道--王卡提成与cbss侧宽带提成
--结果表：select * from tmp_o2o_wangka_fengcun where jifen_month='';
    procedure p_o2o_wangka_yy(p_month in varchar2);

----营业渠道-亲情卡  要等王卡提成明细导出后才跑
--结果表:select * from TMP_o2o_HHB_QQK_fengcun where jifen_month='';
    procedure p_o2o_qinqingka_yy(p_month in varchar2);
     
     
----营业渠道-王卡大使（营业配送）
 ---72小时首次充值金额>=50
 --结果表: select * from TMP_o2o_HHB_XSTC_fengcun where jifen_month='';
    procedure p_o2o_dashi_yy(p_month in varchar2);
    
       
----社会渠道-众由服务外包合同 要等社会渠道促销经理跑完后跑
--结果表:select * from tmp_o2o_zhongyou_fengcun where jifen_month=''
    procedure p_o2o_zhongyou_sh(p_month in varchar2);

    
----社会渠道-红海外包合同 要等计件员工资跑完后跑
--结果表：select * from tmp_honghai_all_fengcun where jifen_month=''
    procedure p_o2o_honghai_sh(p_month in varchar2,kaohe_score in float DEFAULT 0,kaohe_rate in float DEFAULT  1);
    
    
  
    
----社会渠道-联盟主任提成
--结果表: select * from tmp_o2o_lianmeng_fengcun where jifen_month='';
--结果明细表: select * from tmp_o2o_lianmeng_mx_fengcun where jifen_month='';
    procedure p_o2o_lianmeng_sh(p_month in varchar2);
    
----营业渠道-营业配送转化率 二次营销率 等王卡大使跑完后跑
--配送员转化率结果表:select * from tmp_o2o_hhb_zhuanhua_person_bk where jifen_month='';
--厅经理转化率结果表:select * from tmp_o2o_hhb_zhuanhua_group_bk where jifen_month='';
--转化率明细表:select * from tmp_o2o_hhb_zhuanhua_mingxi_bk where jifen_month='';

--配送二次营销率结果表: select * from tmp_o2o_hhb_peisong_bk where jifen_month='';
--配送二次营销率明细表: select * from tmp_o2o_hhb_peisong_mingxi_bk where jifen_month='';

    procedure p_o2o_peisong_yy(p_month varchar2);
    
    
         
     
----最终运行脚本
    procedure p_o2o_run_all(p_month in varchar2); 
      
     
------导入数据------  
/*

--社会-促销经理 导入数据

truncate table zhl_o2o_cuxiao_base_copy;

select * from zhl_o2o_cuxiao_base_copy for update;

insert into tmp_o2o_cuxiao_base
select '1195',to_char(sysdate,'yyyymmddhh24mi'),
x.jifen_month,x.remark,x.staff_kpi,x.staff_salary,
x.staff_area,x.zhixiao_tag,x.staff_id,x.staff_name
from zhl_o2o_cuxiao_base_copy x;


--社会-督导 导入数据

truncate table zhl_o2o_dudao_relation_cp;

select * from zhl_o2o_dudao_relation_cp for update;

insert into tmp_o2o_dudao_relation 
select '1195',to_char(sysdate,'yyyymmddhh24mi'),
a.jifen_month,a.remark,a.staff_id_b,a.staff_name_b,a.staff_id,a.staff_area,a.staff_name
from zhl_o2o_dudao_relation_cp a
     
     
--社会-项目经理 导入数据

truncate table zhl_o2o_xiangmu_base_x;

select * from zhl_o2o_xiangmu_base_x for update;

insert into tmp_o2o_xiangmu_base
select '1195',to_char(sysdate,'yyyymmddhh24mi'),jifen_month,
remark,xundian_salary,staff_kpi,staff_salary,staff_area,
zhixiao_tag,staff_id,'', staff_id_a,staff_name
from zhl_o2o_xiangmu_base_x ;


----------
--政企-地推员 导入数据
truncate table zhl_o2o_ditui_base_cp;

select * from zhl_o2o_ditui_base_cp for update;

insert into tmp_o2o_ditui_base
select '1195',to_char(sysdate,'yyyymmddhh24mi'),
x.jifen_month,x.remark,x.staff_kpi,x.staff_salary,
x.staff_area,x.zhixiao_tag,x.staff_id,x.staff_name
from zhl_o2o_ditui_base_cp x;


    
--政企-地推督导 导入数据

truncate table zhl_o2o_dudao_dt_relation_cp;

select * from zhl_o2o_dudao_dt_relation_cp for update;

insert into tmp_o2o_dudao_dt_relation
select '1195',to_char(sysdate,'yyyymmddhh24mi'),
x.jifen_month,x.remark,x.staff_id_b,x.staff_name_b,
x.staff_id,x.staff_area,x.staff_name
from zhl_o2o_dudao_dt_relation_cp x



--政企-兼职生 导入数据
delete from tmp_o2o_jianzhi_base where jifen_month='201812';
commit;
truncate table zhl_o2o_jianzhi_base_copy ;

select * from zhl_o2o_jianzhi_base_copy for update;

insert into tmp_o2o_jianzhi_base 
select '1195',to_char(sysdate,'yyyymmddhh24mi'),
y.jifen_month,y.remark,y.chuqin_day,y.staff_area,y.staff_id,
y.staff_name
from zhl_o2o_jianzhi_base_copy y;



--家互营业政企王卡提成，宽带 导入数据

delete from tmp_o2o_wangka_base  where jifen_month='201811';
truncate table tmp_o2o_wangka_base_copy;

select * from tmp_o2o_wangka_base_copy for update;

insert into tmp_o2o_wangka_base 
select * from tmp_o2o_wangka_base_copy;



--营业渠道-(王卡大使)营业配送 导入数据
delete from TMP_o2o_HHB_XSTC_base1 where jifen_month='201811';

truncate table TMP_o2o_HHB_XSTC_base_copy ;

select * from TMP_o2o_HHB_XSTC_base_copy for update;

insert into TMP_o2o_HHB_XSTC_base1
select * from TMP_o2o_HHB_XSTC_base_copy;




--社会渠道-联盟主任 导入数据
delete from tmp_o2o_lianmeng_base where jifen_month='201811';

truncate table tmp_o2o_lianmeng_base_copy ;

select * from tmp_o2o_lianmeng_base_copy for update;

insert into tmp_o2o_lianmeng_base
select * from tmp_o2o_lianmeng_base_copy;


--社会渠道-众由合同  导入数据
delete from tmp_o2o_zhongyou_cuxiao where jifen_month='201811';

truncate table tmp_o2o_zhongyou_cuxiao_copy ;

select * from tmp_o2o_zhongyou_cuxiao_copy for update;

insert into tmp_o2o_zhongyou_cuxiao
select * from tmp_o2o_zhongyou_cuxiao_copy;


truncate table tmp_o2o_zhongyou_base_cp;  --每月要导入两次
delete from tmp_o2o_zhongyou_base where jifen_month='201811';
select * from tmp_o2o_zhongyou_base_cp for update;
insert into tmp_o2o_zhongyou_base
select * from tmp_o2o_zhongyou_base_cp;


--社会渠道-红海外包合同 导入最新产品数据
select * from tmp_o2o_honghai_product for update;



--营业渠道-配送营销与转化率  导入数据
delete from tmp_o2o_hhb_zhuanhua_base where jifen_month='201811';

truncate table tmp_o2o_hhb_zhuanhua_base_cp ;

select * from tmp_o2o_hhb_zhuanhua_base_cp for update;

insert into tmp_o2o_hhb_zhuanhua_base
select * from tmp_o2o_hhb_zhuanhua_base_cp;






*/
---------o2o薪酬重置
/*  


---社会-促销经理
--delete from tmp_o2o_cuxiao_base where jifen_month = '201810' 

delete from tmp_o2o_cuxiao_jfbk 
where jifen_month = '201810'                      
         
                    
delete from tmp_o2o_cuxiao_xinchou_fengcun 
where jifen_month = '201810'                       
          
delete from tmp_o2o_cuxiao_fengcun 
where jifen_month = '201810'                       


---社会-督导经理
--delete from tmp_o2o_dudao_relation where jifen_month = '201810' 


delete from tmp_o2o_dudao_xinchou_fengcun 
where jifen_month = '201810'                       
           
           
delete from tmp_o2o_dudao_hffc_fengcun 
where p_month = '201810'                       
           
             
delete from tmp_o2o_dudao_hffc 
where jifen_month = '201810' 


---社会-项目经理
--delete from tmp_o2o_xiangmu_base where jifen_month = '201810' 

delete from tmp_o2o_xiangmu_xc_fengcun 
where jifen_month = '201810';

                       
delete from tmp_o2o_xiangmu_jfbk 
where jifen_month = '201810'                       
         
           
delete from tmp_o2o_xiangmu_fengcun 
where jifen_month = '201810'


----------------------

----政企-地推经理
--delete from tmp_o2o_ditui_base where jifen_month='201810' ;


delete from tmp_o2o_ditui_xinchou_fengcun
where jifen_month = '201810' ;                      
   
                              
delete from tmp_o2o_ditui_jfbk 
where jifen_month = '201810' ;
             
delete from tmp_o2o_ditui_fengcun 
where jifen_month = '201810'


----政企-地推督导
--delete from tmp_o2o_dudao_dt_relation where jifen_month='201810' ;


delete from tmp_o2o_dt_xinchou_fengcun 
where jifen_month = '201810'
             

delete from tmp_o2o_dt_hffc_fengcun 
where p_month = '201810'                       
    

delete from tmp_o2o_dudao_dt_hffc 
where jifen_month = '201810'                       

  
----政企-兼职生
--delete from tmp_o2o_jianzhi_base where jifen_month='201810' ;
           
delete from tmp_o2o_jz_xinchou_fengcun 
where jifen_month = '201810'                       
        
                      
delete from tmp_o2o_jianzhi_jfbk 
where jifen_month = '201810'                       


                      
delete from tmp_o2o_jianzhi_fengcun 
where jifen_month = '201810'  



*/



      

end pkg_chnl_o2o_data;
/
create or replace package body pkg_chnl_o2o_data is

  PROCEDURE safely_drop( in_name IN VARCHAR2 ) AS
    
  v_sql varchar2(500);
  
  BEGIN
    FOR v IN (SELECT *
                FROM all_objects
               WHERE owner = upper('gz_yxcb')
                 AND object_name = upper(in_name)) LOOP
           
      v_sql := 'DROP ' || v.object_type || ' ' || upper('gz_yxcb') || '.' ||
                        upper(in_name) ;
          
      EXECUTE IMMEDIATE v_sql;
    END LOOP;
  END;
  
  ---准备数据
  
  procedure p_o2o_prepare_data(p_month in varchar2) is
    
         v_start_date date ; 
         v_pkg_name varchar2(50);
         v_prg_name varchar2(50);
         v_run_order number(5);
         v_remark varchar2(2000);
         v_end_flag number(1);         
         v_sql clob;
         v_mon_last varchar2(20);
         v_mon_last_num number(5);
         p_month_num number(5);
         
         
         --v_count number(2);

     begin

     --log    
         v_start_date := sysdate ;
         v_pkg_name := 'pkg_chnl_o2o_data' ;
         v_prg_name := 'p_o2o_prepare_data' ;
         v_run_order := 0 ;
         v_mon_last := to_char(add_months(to_date(p_month,'yyyymm'),-1),'yyyymm') ; 
         v_mon_last_num := to_number(to_char(add_months(to_date(v_mon_last,'yyyymm'),-0),'mm')) ;
         p_month_num := to_number(to_char(add_months(to_date(p_month,'yyyymm'),-0),'mm')) ;
         
        
         IF p_month IS  NULL or length(p_month) <> 6 THEN
            raise_application_error(-20001,'p_month is error');
         END IF ;
         
    --cbss当月与上月充值数据
         
     v_sql := 'create table tmp_o2o_paylog nologging as
           select to_char(a.recv_time,''yyyymm'')  pay_mon,
               a.user_id,a.recv_fee,a.recv_time,a.payment_op
          from CBSSA_UCR_ACT3.TF_B_PAYLOG@link_4gbss_all a
          where partition_id in (''' || v_mon_last_num || ''',''' || p_month_num || ''') 
               and a.recv_time >= to_date('''|| v_mon_last ||'01'',''yyyymmdd'')
               and a.cancel_tag =''0''
               and a.pay_fee_mode_code <> 4 
               and a.payment_op in (16000,16001)
               and eparchy_code = ''0020''        
         ';
         
      safely_drop('tmp_o2o_paylog');
      execute immediate v_sql ;
         
      v_run_order := v_run_order + 1 ;
      v_end_flag := 1 ;
      v_remark := 'create tmp_o2o_paylog(当月上月cbss充值数据) OK  ';
      insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
      commit ;   
      
      
     --bss月与上月充值数据
      v_sql := 'create table tmp_o2o_bss_paylog nologging as
           select to_char(a.recv_time,''yyyymm'')  pay_mon,
               a.user_id,a.recv_fee,a.recv_time,a.payment_op
          from act_UCR_ACT1.TF_B_PAYLOG@link_ngbss a
          where partition_id in (''' || v_mon_last_num || ''',''' || p_month_num || ''') 
               and a.recv_time >= to_date('''|| v_mon_last ||'01'',''yyyymmdd'')
               and a.cancel_tag =''0''
               and a.pay_fee_mode_code <> 4 
               and a.payment_op in (16000,16001)
               and eparchy_code = ''0020''        
         ';
         
      safely_drop('tmp_o2o_bss_paylog');
      execute immediate v_sql ;
         
      v_run_order := v_run_order + 1 ;
      v_end_flag := 1 ;
      v_remark := 'create tmp_o2o_bss_paylog(当月上月bss充值数据) OK  ';
      insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
      commit ;
     
      
      
      
      --省三无指标数据 每月8号左右生成（status为1代表是三无用户）
          
      v_sql := 'create table tmp_o2o_sw nologging as
                select area_code,subs_id as user_id,attr_code,attr_value_code,status
                from gdyj.T34_002_' || p_month || '@link_typt_new a
                where area_code =''A''
                and a.attr_value_code = ''UNLC0809''        
         ';
         
      safely_drop('tmp_o2o_sw');
      execute immediate v_sql ;
      
      v_run_order := v_run_order + 1 ;
      v_end_flag := 1 ;
      v_remark := 'create tmp_o2o_sw(当月三无指标数据) OK  ';
      insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
      commit ; 
      
      
      --上上月的省三无指标数据
      v_sql := 'create table tmp_o2o_sw_last nologging as
                select area_code,subs_id as user_id,attr_code,attr_value_code,status
                from gdyj.T34_002_' || v_mon_last || '@link_typt_new a
                where area_code =''A''
                and a.attr_value_code = ''UNLC0809''        
         '; 
         
      safely_drop('tmp_o2o_sw_last');
      execute immediate v_sql ;
      
      v_run_order := v_run_order + 1 ;
      v_end_flag := 1 ;
      v_remark := 'create tmp_o2o_sw_last(上月三无指标数据) OK  ';
      insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
      commit ; 
      
      
      ---省是否省外漫游指标数据 每月8号前生成上月的
      v_sql := 'create table tmp_o2o_my nologging as
                select area_code,subs_id as user_id,serial_number,type,''1'' status
                from gdyj.T34_GD_ROAM_' || p_month || '@link_typt_new a
                where area_code =''A''
                and a.type in (1,2,3,4,6,7)       
         '; 
         
      safely_drop('tmp_o2o_my');
      execute immediate v_sql ;
      
      v_run_order := v_run_order + 1 ;
      v_end_flag := 1 ;
      v_remark := 'create tmp_o2o_my(当月省外漫游指标数据) OK  ';
      insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
      commit ; 
              
               
   exception 
        when others then     
           v_run_order := v_run_order + 1 ;         
           v_end_flag :=  0;
           v_remark := sqlerrm ;
           insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark);
           commit ;  
   --   raise_application_error(-20099,v_remark);  
   end p_o2o_prepare_data ;
   
   
 ----------------   
   
 --- 社会渠道-O2O促销经理 
   
   procedure p_o2o_cuxiao_sh(p_month in varchar2) is
    
         v_start_date date ; 
         v_pkg_name varchar2(50);
         v_prg_name varchar2(50);
         v_run_order number(5);
         v_remark varchar2(2000);
         v_end_flag number(1);         
         v_sql clob;
         v_mon_last varchar2(50);

         --v_count number(2);

     begin

     --log   
      
         v_start_date := sysdate ;
         v_pkg_name := 'pkg_chnl_o2o_data' ;
         v_prg_name := 'p_o2o_cuxiao_sh' ;
         v_run_order := 0 ;  
         v_mon_last := to_char(add_months(to_date(p_month,'yyyymm'),-1),'yyyymm') ;
        
         IF p_month IS NULL or length(p_month) <> 6 THEN
            raise_application_error(-20001,'p_month is error');
         END IF ;
         
         --清空本月封存表
          v_sql := 'delete from tmp_o2o_cuxiao_jfbk 
           where jifen_month = ''' || p_month || '''                       
            '; 
             
           execute immediate v_sql ;
           commit;
                    
           v_sql := 'delete from tmp_o2o_cuxiao_xinchou_fengcun 
           where jifen_month = ''' || p_month || '''                       
            '; 
             
           execute immediate v_sql ;
           commit;
                     
           v_sql := 'delete from tmp_o2o_cuxiao_fengcun 
           where jifen_month = ''' || p_month || '''                       
            '; 
             
           execute immediate v_sql ;
           commit;
           
         
         ---当月发展用户
         
         v_sql := ' create table tmp_o2o_cuxiao_user nologging as
         select a.jifen_month,a.staff_name,a.staff_id,a.zhixiao_tag,
         a.staff_area,a.staff_salary,a.staff_kpi,a.remark,
         b.user_id,b.serial_number,b.cust_id,b.remove_tag,
         b.user_state_name,b.deal_dt,b.product_id,b.product_name
         from tmp_o2o_cuxiao_base a,report.t_user_all_'|| p_month ||' b
         where a.staff_id=b.cbss_developer_id and a.jifen_month = ''' || p_month || '''
               and to_char(b.deal_dt,''yyyymm'') = ''' || p_month || '''         
         ';
         safely_drop('tmp_o2o_cuxiao_user');
         
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_cuxiao_user(当月促销员发展用户数据) OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ; 
         
         
         ---当月的上月不满足条件的当月继续观察
         v_sql := 'insert into tmp_o2o_cuxiao_user
         select distinct a.jifen_month,a.staff_name,
         a.staff_id,a.zhixiao_tag,a.staff_area,
         a.staff_salary,a.staff_kpi,a.remark,a.user_id,serial_number,cust_id,remove_tag,
         user_state_name,deal_dt,product_id,product_name
         from tmp_o2o_cuxiao_jfbk a
         where jifen_month = ''' || v_mon_last || '''                
         ';
         
         execute immediate v_sql ;
         commit;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'insert tmp_o2o_cuxiao_user OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ; 
         
         --- 取当月累计缴费 201902改为看T+3日内首充
         /*v_sql := 'create table tmp_o2o_cuxiao_paylog nologging as
         select b.user_id,sum(b.recv_fee)/100 payfee
         from (select distinct * from tmp_o2o_cuxiao_user) a,
         tmp_o2o_paylog b
         where a.user_id = b.user_id
         and b.pay_mon in ( '''|| p_month ||''')
         group by b.user_id    
                       
         ';*/
         
         v_sql := '
         create table tmp_o2o_cuxiao_paylog nologging as
select distinct user_id,first_value(recv_fee/100) OVER(PARTITION BY user_id ORDER BY recv_time) pay_fee
from (
SELECT b.user_id,b.recv_fee,b.recv_time
from tmp_o2o_cuxiao_user a,CBSSA_UCR_ACT3.TF_B_PAYLOG_ADD_BIL b
WHERE a.user_id=b.user_id
AND b.pay_fee_mode_code<>4
AND b.payment_op IN (16000,16001)
and b.recv_fee>0
AND B.RECV_TIME>=A.DEAL_DT
AND B.RECV_TIME< A.DEAL_DT+4 
and to_char(b.recv_time,''yyyymm'')= '''|| p_month ||'''
)  
         
         ';
         
             
         
         safely_drop('tmp_o2o_cuxiao_paylog');
         execute immediate v_sql ;
                
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_cuxiao_paylog(当月促销员累计缴费数据) OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ; 
         
         
         ---取当月踢赠出账数据
         v_sql := 'create table tmp_o2o_cuxiao_bill nologging as
         select b.user_id,b.cycle_id,sum(b.fee+b.adjust_after-b.writeoff_fee1)/100 fee
         from (select distinct * from tmp_o2o_cuxiao_user) a,
         yangyi.ts_b_bill_cbss_' || p_month || ' b
         where a.user_id = b.user_id
         and b.cycle_id = ''' || p_month || '''
         group by b.user_id,b.cycle_id 
                       
         ';        
         
         safely_drop('tmp_o2o_cuxiao_bill');
         execute immediate v_sql ;
                
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_cuxiao_bill(当月促销员出账数据) OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ; 
         
         
         ---汇总1
         v_sql := 'create table tmp_o2o_cuxiao_all_tmp1 nologging as
         select a.*,
         nvl(b.pay_fee,0) payfee,
         nvl(c.fee,0) fee,
         nvl(d.status,0) sw_flag,
         1 valid_user
         from (select distinct * from tmp_o2o_cuxiao_user) a,tmp_o2o_cuxiao_paylog b,
         tmp_o2o_cuxiao_bill c,tmp_o2o_sw d
         where a.user_id=b.user_id(+)
         and a.user_id=c.user_id(+)
         and a.user_id=d.user_id(+) 
                       
         ';     
         
         safely_drop('tmp_o2o_cuxiao_all_tmp1');
         execute immediate v_sql ;
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_cuxiao_all_tmp1 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         ---汇总2
         v_sql := 'create table tmp_o2o_cuxiao_all_tmp2 nologging as
         select a.*,''o2o促销经理'' as jf_type,
         (case when a.payfee >=50 and a.fee >0 
         and a.sw_flag = 0 and a.user_state_name like ''%正常%'' then 1 else 0 end) chk_flag  
         from tmp_o2o_cuxiao_all_tmp1 a 
                       
         ';     
         
         safely_drop('tmp_o2o_cuxiao_all_tmp2');
         execute immediate v_sql ;
         
         --插入到表中做封存数据
         v_sql := 'insert into tmp_o2o_cuxiao_fengcun
         select distinct a.*
         from tmp_o2o_cuxiao_all_tmp2 a
         --where a.chk_flag=''1''
                       
         '; 
             
         execute immediate v_sql ;
         commit;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_cuxiao_all_tmp2 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ; 
         
         
         ---备份当月不满足条件的号码
         v_sql := 'insert into tmp_o2o_cuxiao_jfbk 
         select distinct a.*
         from tmp_o2o_cuxiao_all_tmp2 a
         where a.payfee <50 and jifen_month = ''' || p_month || '''
                       
         '; 
             
         execute immediate v_sql ;
         commit;
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'insert tmp_o2o_cuxiao_jfbk OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         ---汇总最终积分
         v_sql := 'create table tmp_o2o_cuxiao_jifen nologging as
        select distinct a.*,
         (case when  a.payfee >=50 and a.payfee<100 then 30 
         when a.payfee >=100 and a.payfee<200 then 50 
         when a.payfee >=200 then 80
           else 0 end) as jifen_rate
         from tmp_o2o_cuxiao_all_tmp2 a 
         where chk_flag>=1 and product_name not like ''%亲情卡%''
         union 
         select distinct a.*,
         (case when a.sw_flag = 0 /*and a.my_flag = 0*/ and a.user_state_name like ''%正常%'' then 10 
           else 0 end) as jifen_rate
         from tmp_o2o_cuxiao_all_tmp2 a 
         where product_name  like ''%亲情卡%''
                       
         '; 
         
           
         safely_drop('tmp_o2o_cuxiao_jifen');
         execute immediate v_sql ;
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_cuxiao_jifen OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ; 
       
         ---算出促销员的当月产能 并取得积分薪酬
         
         v_sql := 'create table tmp_o2o_cuxiao_channeng nologging as
         select a.*,nvl(b.cn,0)as cn,
         (case when  b.zhixiao_tag=1 then  nvl(b.jf_rate,0)  else 0 end ) as jf_xinchou
         from tmp_o2o_cuxiao_base a,
         (
           select 
           a.staff_id,a.zhixiao_tag,sum(a.chk_flag) as cn,sum(a.jifen_rate) as jf_rate,
           ''' || p_month || ''' as cn_month
           from tmp_o2o_cuxiao_jifen a
           group by a.staff_id,a.zhixiao_tag
         )  b
         where a.staff_id=b.staff_id(+) and a.jifen_month = ''' || p_month || '''
                       
         ';  
         
         
         safely_drop('tmp_o2o_cuxiao_channeng');
         execute immediate v_sql ;
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_cuxiao_channeng OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         ----计算其他薪酬
         v_sql := 'create table tmp_o2o_cuxiao_xinchou1 nologging as
         select a.*,
         (case when substr(a.jifen_month,-2) between ''05'' and ''10'' then 150 else 0 end) as gw_butie,
         (case when b.cycle_id= a.jifen_month then round(450*nvl(b.rate,0),2) else 0 end) as dt_butie
         from tmp_o2o_cuxiao_channeng a,(select * from shensw3.t_o2o_saler_months where cycle_id=''' || p_month || ''') b
         where a.staff_id=b.develop_staff_id(+) and a.remark not like ''%督导%'' 
                       
         ';     
         
         safely_drop('tmp_o2o_cuxiao_xinchou1');
         execute immediate v_sql ;
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_cuxiao_xinchou1 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         ---计算总薪酬并放入封存表中
         v_sql := 'create table tmp_o2o_cuxiao_xinchou nologging as
         select a.*,
         (a.staff_salary+a.jf_xinchou*a.staff_kpi+gw_butie+dt_butie) as cuxiao_xinchou,
         (a.staff_salary|| ''+'' ||a.dt_butie || ''+'' || a.gw_butie||''+''||a.staff_kpi||''*''||a.jf_xinchou ) compulation
         from tmp_o2o_cuxiao_xinchou1 a  
                       
         ';     
         
         safely_drop('tmp_o2o_cuxiao_xinchou');
         execute immediate v_sql ;
         
         v_sql := 'insert into tmp_o2o_cuxiao_xinchou_fengcun
         select * from tmp_o2o_cuxiao_xinchou
         ';
         execute immediate v_sql ;
         commit;
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_cuxiao_xinchou(取得总薪酬) OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
           
  
      exception 
        when others then 
               
           v_run_order := v_run_order + 1 ;         
           v_end_flag :=  0;
           v_remark := sqlerrm ;
           insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark);
           commit ; 
           
           v_sql := 'delete from tmp_o2o_cuxiao_jfbk 
           where jifen_month = ''' || p_month || '''                       
            '; 
             
           execute immediate v_sql ;
           commit;
                    
           v_sql := 'delete from tmp_o2o_cuxiao_xinchou_fengcun 
           where jifen_month = ''' || p_month || '''                       
            '; 
             
           execute immediate v_sql ;
           commit;
                     
           v_sql := 'delete from tmp_o2o_cuxiao_fengcun 
           where jifen_month = ''' || p_month || '''                       
            '; 
             
           execute immediate v_sql ;
           commit;
                     
            
   --   raise_application_error(-20099,v_remark);  
   
   end p_o2o_cuxiao_sh ;
   
   
 ----------------
 ---社会渠道-O2O督导经理 
 
   procedure p_o2o_dudao_sh(p_month in varchar2) is
         v_start_date date ; 
         v_pkg_name varchar2(50);
         v_prg_name varchar2(50);
         v_run_order number(5);
         v_remark varchar2(2000);
         v_end_flag number(1);         
         v_sql clob;
         v_mon_last varchar2(50);
         v_mon_lastsix varchar2(50);

         --v_count number(2);

     begin

     --log   
      
         v_start_date := sysdate ;
         v_pkg_name := 'pkg_chnl_o2o_data' ;
         v_prg_name := 'p_o2o_dudao_sh' ;
         v_run_order := 0 ;  
         v_mon_last := to_char(add_months(to_date(p_month,'yyyymm'),-1),'yyyymm') ;
         v_mon_lastsix := to_char(add_months(to_date(p_month,'yyyymm'),-6),'yyyymm') ;
        
         IF p_month IS NULL or length(p_month) <> 6 THEN
            raise_application_error(-20001,'p_month is error');
         END IF ;
         
         ---清空本月封存表状态
           v_sql := 'delete from tmp_o2o_dudao_xinchou_fengcun 
           where jifen_month = ''' || p_month || '''                       
            '; 
             
           execute immediate v_sql ;
           commit;
           
           v_sql := 'delete from tmp_o2o_dudao_hffc_fengcun 
           where p_month = ''' || p_month || '''                       
            '; 
             
           execute immediate v_sql ;
           commit;
           
           v_sql := 'delete from tmp_o2o_dudao_hffc 
           where jifen_month = ''' || p_month || '''                       
            '; 
             
           execute immediate v_sql ;
           commit;
         
         
         
         ---取得督导员下属促销经理的直销积分并计算总产能
         
         v_sql := ' create table tmp_o2o_dudao_channeng nologging as
         select x.jifen_month,x.staff_name,x.staff_area,x.staff_id,sum(cn) dudao_cn 
         from ( select a.*,nvl(b.cn,0) as cn
         from tmp_o2o_dudao_relation a,tmp_o2o_cuxiao_channeng b
         where a.staff_id_b=b.staff_id(+) and b.remark not like ''%督导%'' and a.jifen_month = ''' || p_month || ''' ) x
         group by x.jifen_month,x.staff_name,x.staff_area,x.staff_id     
         ';
         safely_drop('tmp_o2o_dudao_channeng');
         
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_dudao_channeng OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         ---话费分成  取前6个月开户的user,并把本月数据插入到历史表中
         v_sql := ' create table tmp_o2o_dudao_user nologging as
         select distinct a.*,b.user_id,b.serial_number,b.cust_id,b.remove_tag,
         b.user_state_name,b.deal_dt,b.product_id,b.product_name
         from tmp_o2o_dudao_channeng a,tmp_o2o_cuxiao_user b,
         tmp_o2o_dudao_relation c
         where a.staff_id=c.staff_id 
         and c.staff_id_b=b.staff_id
         and to_char(b.deal_dt,''yyyymm'') = ''' || p_month || '''   
         ';
         safely_drop('tmp_o2o_dudao_user');
         
         execute immediate v_sql ;
         
         v_sql := ' insert into tmp_o2o_dudao_hffc 
         select distinct * from tmp_o2o_dudao_user    
         ';
         
         execute immediate v_sql ;
         commit;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_dudao_user OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         --连续计提6个月 插入历史数据到user表中参与计提
         v_sql := ' insert into tmp_o2o_dudao_user 
         select distinct jifen_month,staff_name,staff_area,staff_id,dudao_cn,
         user_id,serial_number,cust_id,remove_tag,
         user_state_name,deal_dt,product_id,product_name 
         from tmp_o2o_dudao_hffc c
         where c.jifen_month between ''' || v_mon_lastsix || ''' and ''' || v_mon_last || '''    
         ';
         
         execute immediate v_sql ;
         commit;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'insert tmp_o2o_dudao_user OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         ---取出账数据
         
         v_sql := ' create table tmp_o2o_dudao_bill nologging as 
         select b.user_id,b.cycle_id,sum(b.fee+b.adjust_after-b.writeoff_fee1)/100 fee
         from (select distinct * from tmp_o2o_dudao_user) a,yangyi.ts_b_bill_cbss_' || p_month || ' b
         where a.user_id = b.user_id
         and b.cycle_id = ''' || p_month || ''' 
         and to_char(a.deal_dt,''yyyymm'') between ''' || v_mon_lastsix || ''' and ''' || v_mon_last || '''
         group by b.user_id,b.cycle_id    
         ';
         
         safely_drop('tmp_o2o_dudao_bill');
         execute immediate v_sql ;
        
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_dudao_bill OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         ---取三无指标
         
         v_sql := ' create table tmp_o2o_dudao_sw nologging as 
         select a.user_id,nvl(b.status,0) status
         from (select distinct * from tmp_o2o_dudao_user) a,tmp_o2o_sw b
         where a.user_id=b.user_id(+)    
         ';
         
         
         safely_drop('tmp_o2o_dudao_sw');         
         execute immediate v_sql ;
              
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_dudao_sw OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         --得到当月的话费分成
         v_sql := ' create table tmp_o2o_dudao_fencheng nologging as 
         select staff_id,staff_name,sum(fee) hf_fencheng 
         from (select a.*,round(b.fee*0.01,1) fee 
         from (select distinct * from tmp_o2o_dudao_user
         where to_char(deal_dt,''yyyymm'') between ''' || v_mon_lastsix || ''' and ''' || v_mon_last || ''') a,
         tmp_o2o_dudao_bill b,tmp_o2o_dudao_sw c
         where a.user_id=b.user_id
         and a.user_id=c.user_id
         and b.fee>0 and c.status=0 )  
         group by staff_id,staff_name   
         ';
         
         safely_drop('tmp_o2o_dudao_fencheng');         
         execute immediate v_sql ;
              
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_dudao_fencheng OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         ---把话费分成明细放到封存表
         v_sql:= ' insert into tmp_o2o_dudao_hffc_fengcun 
         select a.*, fee,''' || p_month || ''' p_month 
         from (select distinct * from tmp_o2o_dudao_user
         where to_char(deal_dt,''yyyymm'') between ''' || v_mon_lastsix || ''' and ''' || v_mon_last || ''') a,
         tmp_o2o_dudao_bill b,tmp_o2o_dudao_sw c
         where a.user_id=b.user_id
         and a.user_id=c.user_id
         and b.fee>0 and c.status=0    
         ';
         
         execute immediate v_sql ;
         commit;
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'insert tmp_o2o_dudao_hffc_fengcun OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         ---汇总督导员薪酬
         v_sql := ' create table tmp_o2o_dudao_xinchou1 nologging as 
         select distinct b.* ,nvl(c.hf_fencheng,0) as hf_fencheng,nvl(d.dudao_cn,0) dudao_cn
         from (select * from tmp_o2o_cuxiao_channeng where remark like ''%督导%'') b,
         tmp_o2o_dudao_fencheng c,tmp_o2o_dudao_channeng d
         where  b.staff_id=c.staff_id(+) and b.staff_id=d.staff_id(+)   
         ';
         
         safely_drop('tmp_o2o_dudao_xinchou1');         
         execute immediate v_sql ;
              
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_dudao_xinchou1 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         ---计算最终薪酬
         v_sql := ' create table tmp_o2o_dudao_xinchou nologging as 
         select distinct a.*,
         (a.staff_salary + a.jf_xinchou*a.staff_kpi + a.hf_fencheng*a.staff_kpi +
          a.dudao_cn*4*a.staff_kpi) as dudao_xinchou,
         (a.staff_salary|| ''+'' ||a.staff_kpi||''*''||a.jf_xinchou || ''+'' ||a.staff_kpi||''*''||a.hf_fencheng || ''+'' ||a.staff_kpi||''*''||a.dudao_cn|| ''*''||''4'' ) compulation
          from tmp_o2o_dudao_xinchou1 a   
         ';
         
         safely_drop('tmp_o2o_dudao_xinchou');         
         execute immediate v_sql ;
         
         v_sql:= 'insert into tmp_o2o_dudao_xinchou_fengcun 
         select * from tmp_o2o_dudao_xinchou  
         ';
         execute immediate v_sql ;
         commit;     
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_dudao_xinchou OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
                
           
   exception 
        when others then     
           v_run_order := v_run_order + 1 ;         
           v_end_flag :=  0;
           v_remark := sqlerrm ;
           
           insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark);
           commit ;
           
           
           v_sql := 'delete from tmp_o2o_dudao_xinchou_fengcun 
           where jifen_month = ''' || p_month || '''                       
            '; 
             
           execute immediate v_sql ;
           commit;
           
           v_sql := 'delete from tmp_o2o_dudao_hffc_fengcun 
           where p_month = ''' || p_month || '''                       
            '; 
             
           execute immediate v_sql ;
           commit;
           
           v_sql := 'delete from tmp_o2o_dudao_hffc 
           where jifen_month = ''' || p_month || '''                       
            '; 
             
           execute immediate v_sql ;
           commit;
           
           
               
   
   end p_o2o_dudao_sh ;  
   
---------------- 
---社会渠道-O2O项目经理 
   
   procedure p_o2o_xiangmu_sh(p_month in varchar2) is
     
         v_start_date date ; 
         v_pkg_name varchar2(50);
         v_prg_name varchar2(50);
         v_run_order number(5);
         v_remark varchar2(2000);
         v_end_flag number(1);         
         v_sql clob;
         v_mon_last varchar2(50);

         --v_count number(2);

     begin

     --log   
      
         v_start_date := sysdate ;
         v_pkg_name := 'pkg_chnl_o2o_data' ;
         v_prg_name := 'p_o2o_xiangmu_sh' ;
         v_run_order := 0 ;  
         v_mon_last := to_char(add_months(to_date(p_month,'yyyymm'),-1),'yyyymm') ;
        
         IF p_month IS NULL or length(p_month) <> 6 THEN
            raise_application_error(-20001,'p_month is error');
         END IF ;
         
         
         --清空本月封存状态
           v_sql := 'delete from tmp_o2o_xiangmu_xc_fengcun 
           where jifen_month = ''' || p_month || '''                       
            '; 
             
           execute immediate v_sql ;
           commit;
           
           v_sql := 'delete from tmp_o2o_xiangmu_jfbk 
           where jifen_month = ''' || p_month || '''                       
            '; 
             
           execute immediate v_sql ;
           commit;
           
           v_sql := 'delete from tmp_o2o_xiangmu_fengcun 
           where jifen_month = ''' || p_month || '''                       
            '; 
             
           execute immediate v_sql ;
           commit;
         
         
         
         ---根据发展人编码 找到对应的上月开户的user
         
         v_sql := ' create table tmp_o2o_xiangmu_user nologging as 
         select distinct a.jifen_month,a.staff_name,a.staff_id_a,a.staff_name_b,
         a.staff_id,a.zhixiao_tag,a.staff_area,a.staff_salary,a.staff_kpi,a.remark,b.user_id,serial_number,cust_id,remove_tag,
         user_state_name,deal_dt,product_id,product_name
         from tmp_o2o_xiangmu_base a,report.t_user_all_'|| p_month ||' b
         where a.staff_id=b.cbss_developer_id and a.jifen_month = ''' || p_month || '''
         and to_char(b.deal_dt,''yyyymm'') = ''' || p_month  || '''
             
         ';
         
         safely_drop('tmp_o2o_xiangmu_user');         
         execute immediate v_sql ;
              
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_xiangmu_user OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         --取上月不满足条件的用户 本月继续观察
         v_sql := 'insert into tmp_o2o_xiangmu_user
         select distinct a.jifen_month,a.staff_name,a.staff_id_a,a.staff_name_b,
         a.staff_id,a.zhixiao_tag,a.staff_area,
         a.staff_salary,a.staff_kpi,a.remark,a.user_id,serial_number,cust_id,remove_tag,
         user_state_name,deal_dt,product_id,product_name
         from tmp_o2o_xiangmu_jfbk a
         where jifen_month = ''' || v_mon_last || '''                
         ';
         
         execute immediate v_sql ;
         commit;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'insert tmp_o2o_xiangmu_user OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ; 
         
          
         --- 取当月累计缴费
         v_sql := 'create table tmp_o2o_xiangmu_paylog nologging as
         select b.user_id,sum(b.recv_fee)/100 payfee
         from (select distinct * from tmp_o2o_xiangmu_user) a,
         tmp_o2o_paylog b
         where a.user_id = b.user_id
         and b.pay_mon in ( '''|| p_month ||''')
         group by b.user_id    
                       
         ';
         safely_drop('tmp_o2o_xiangmu_paylog');
         execute immediate v_sql ;
                
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_xiangmu_paylog(当月项目经理累计缴费数据) OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ; 
         
         --- 取当月出账
         
         v_sql := 'create table tmp_o2o_xiangmu_bill nologging as
         select b.user_id,b.cycle_id,sum(b.fee)/100 fee
         from (select distinct * from tmp_o2o_xiangmu_user) a,
         yangyi.ts_b_bill_cbss_'|| p_month ||' b
         where a.user_id = b.user_id
         and b.cycle_id = '''|| p_month ||'''
         group by b.user_id,b.cycle_id    
                       
         ';
         safely_drop('tmp_o2o_xiangmu_bill');
         execute immediate v_sql ;
                
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_xiangmu_bill(当月项目经理出账数据) OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         ---汇总1
         v_sql := 'create table tmp_o2o_xiangmu_all_tmp nologging as
         select a.*,nvl(b.payfee,0) payfee,nvl(c.fee,0) fee,
         nvl(d.status,0) sw_flag,
         1 valid_user
         from (select distinct * from tmp_o2o_xiangmu_user) a,tmp_o2o_xiangmu_paylog b,
         tmp_o2o_xiangmu_bill c,tmp_o2o_sw d
         where a.user_id=b.user_id(+)
         and a.user_id=c.user_id(+)
         and a.user_id=d.user_id(+)
    
                       
         ';
         safely_drop('tmp_o2o_xiangmu_all_tmp');
         execute immediate v_sql ;
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_xiangmu_all_tmp OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
                
         ---汇总2
         v_sql := 'create table tmp_o2o_xiangmu_all nologging as
         select a.*,''o2o项目经理'' as jf_type,
         (case when a.payfee >=50 and a.fee >0 
          and a.sw_flag = 0 and a.user_state_name like ''%正常%'' then 1 else 0 end) chk_flag
          from tmp_o2o_xiangmu_all_tmp a    
                       
         ';
         safely_drop('tmp_o2o_xiangmu_all');
         execute immediate v_sql ;
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_xiangmu_all OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ; 
         
         --插入到表中做封存数据
         v_sql := 'insert into tmp_o2o_xiangmu_fengcun
         select distinct a.*
         from tmp_o2o_xiangmu_all a
         --where a.chk_flag=''1''
                       
         ';
         execute immediate v_sql ;
         commit;
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'insert tmp_o2o_xiangmu_fengcun OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ; 
         
         --备份当月不满足条件的号码
         
         v_sql := 'insert into tmp_o2o_xiangmu_jfbk 
         select a.*
         from tmp_o2o_xiangmu_all a
         where a.payfee <50 and jifen_month = '''|| p_month ||'''   
                       
         ';
         
         execute immediate v_sql ;
         commit;
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'insert tmp_o2o_xiangmu_all OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         --汇总最终积分
         
         v_sql := 'create table tmp_o2o_xiangmu_jifen nologging as
         select a.*
         from tmp_o2o_xiangmu_all a 
         where chk_flag=1    
                       
         ';
         safely_drop('tmp_o2o_xiangmu_jifen');
         execute immediate v_sql ;
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_xiangmu_jifen OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         ---算出促销员的当月产能 并取得积分薪酬
         
         v_sql := 'create table tmp_o2o_xiangmu_channeng nologging as
         select jifen_month,staff_name,staff_id_a,sum(cn) cn,sum(jf_xinchou) jf_xinchou from (
         select a.*,nvl(b.cn,0)as cn,
         (case when  b.zhixiao_tag=1 then  nvl(b.cn,0)*30  else nvl(b.cn,0)*15 end ) as jf_xinchou
         from tmp_o2o_xiangmu_base a,
         (select a.staff_id,a.zhixiao_tag,sum(a.chk_flag) as cn,'''|| p_month ||''' as cn_month
         from tmp_o2o_xiangmu_jifen a
         group by a.staff_id,a.zhixiao_tag) b
         where a.staff_id=b.staff_id(+) and a.jifen_month= '''|| p_month ||''')
         group by staff_name,staff_id_a,jifen_month   
                       
         ';
         safely_drop('tmp_o2o_xiangmu_channeng');
         execute immediate v_sql ;
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_xiangmu_channeng OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         ----计算其他薪酬
         
         v_sql := 'create table tmp_o2o_xiangmu_xinchou1 nologging as
         select distinct a.*,
        (case when substr(a.jifen_month,-2) between ''05'' and ''10'' then 150 else 0 end) as gw_butie,
        b.xundian_salary,b.staff_kpi,b.staff_salary
        from tmp_o2o_xiangmu_channeng a,tmp_o2o_xiangmu_base b
        where a.staff_id_a=b.staff_id_a   and b.jifen_month= '''|| p_month ||'''                       
         ';
         
         safely_drop('tmp_o2o_xiangmu_xinchou1');
         execute immediate v_sql ;
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_xiangmu_xinchou1 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         ---计算总薪酬
         v_sql := 'create table tmp_o2o_xiangmu_xinchou nologging as
         select a.*,
         (a.staff_salary+a.jf_xinchou*a.staff_kpi+gw_butie+a.xundian_salary) as xiangmu_xinchou,
         (a.staff_salary||''+'' || ''+''|| a.gw_butie||''+''||a.staff_kpi||''*''||a.jf_xinchou||''+''||a.xundian_salary ) compulation
         from tmp_o2o_xiangmu_xinchou1 a                        
         ';
         
         safely_drop('tmp_o2o_xiangmu_xinchou');
         execute immediate v_sql ;
         
         v_sql:= 'insert into tmp_o2o_xiangmu_xc_fengcun 
         select * from tmp_o2o_xiangmu_xinchou  
         ';
         execute immediate v_sql ;
         commit;     
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_xiangmu_xinchou OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         

   exception 
        when others then     
           v_run_order := v_run_order + 1 ;         
           v_end_flag :=  0;
           v_remark := sqlerrm ;
           insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark);
           commit ; 
           
           v_sql := 'delete from tmp_o2o_xiangmu_xc_fengcun 
           where jifen_month = ''' || p_month || '''                       
            '; 
             
           execute immediate v_sql ;
           commit;
           
           v_sql := 'delete from tmp_o2o_xiangmu_jfbk 
           where jifen_month = ''' || p_month || '''                       
            '; 
             
           execute immediate v_sql ;
           commit;
           
           v_sql := 'delete from tmp_o2o_xiangmu_fengcun 
           where jifen_month = ''' || p_month || '''                       
            '; 
             
           execute immediate v_sql ;
           commit;
           
           
             
  
   end p_o2o_xiangmu_sh ; 
   
 ----------------
 --政企渠道-O2O地推经理 
   
   procedure p_o2o_ditui_zq(p_month in varchar2) is
     
         v_start_date date ; 
         v_pkg_name varchar2(50);
         v_prg_name varchar2(50);
         v_run_order number(5);
         v_remark varchar2(2000);
         v_end_flag number(1);         
         v_sql clob;
         --v_mon_last varchar2(50);
         --v_mon_lastsix varchar2(50);

         --v_count number(2);

     begin

     --log   
         
         v_start_date := sysdate ;
         v_pkg_name := 'pkg_chnl_o2o_data' ;
         v_prg_name := 'p_o2o_ditui_zq' ;
         v_run_order := 0 ;  
         --v_mon_last := to_char(add_months(to_date(p_month,'yyyymm'),-1),'yyyymm') ;
        -- v_mon_lastsix := to_char(add_months(to_date(p_month,'yyyymm'),-5),'yyyymm') ;
        
         IF p_month IS NULL or length(p_month) <> 6 THEN
            raise_application_error(-20001,'p_month is error');
         END IF ;
         
         --
         v_sql := 'delete from tmp_o2o_ditui_xinchou_fengcun
           where jifen_month = ''' || p_month || '''                       
            '; 
             
           execute immediate v_sql ;
           commit;
                    
           v_sql := 'delete from tmp_o2o_ditui_jfbk 
           where jifen_month = ''' || p_month || '''                       
            '; 
             
           execute immediate v_sql ;
           commit;
                     
           v_sql := 'delete from tmp_o2o_ditui_fengcun 
           where jifen_month = ''' || p_month || '''                       
            '; 
             
           execute immediate v_sql ;
           commit;
         
         
         
         ---当月发展用户 
         
         v_sql := ' create table tmp_o2o_ditui_user nologging as
         select a.jifen_month,a.staff_name,a.staff_id,a.zhixiao_tag,
         a.staff_area,a.staff_salary,a.staff_kpi,a.remark,
         b.user_id,b.serial_number,b.cust_id,b.remove_tag,
         b.user_state_name,b.deal_dt,b.product_id,b.product_name
         from tmp_o2o_ditui_base a,report.t_user_all_'|| p_month ||' b
         where a.staff_id=b.cbss_developer_id and a.jifen_month = ''' || p_month || '''
               and to_char(b.deal_dt,''yyyymm'') = ''' || p_month || '''       
         ';
         safely_drop('tmp_o2o_ditui_user');
         
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_ditui_user(当月促销员发展用户数据) OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ; 
         
              
         /*---当月的上月不满足条件的当月继续观察 201811 改为不看这个指标了
         v_sql := 'insert into tmp_o2o_ditui_user
         select distinct a.jifen_month,a.staff_name,
         a.staff_id,a.zhixiao_tag,a.staff_area,
         a.staff_salary,a.staff_kpi,a.remark,a.user_id,serial_number,cust_id,remove_tag,
         user_state_name,deal_dt,product_id,product_name
         from tmp_o2o_ditui_jfbk a
         where jifen_month = ''' || v_mon_last || '''                
         ';
         
         execute immediate v_sql ;
         commit;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'insert tmp_o2o_ditui_user OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ; */
         
         
         /*--- 取当月累计缴费 201811改为 取激活后72小时内充值。
         v_sql := 'create table tmp_o2o_ditui_paylog nologging as
         select b.user_id,sum(b.recv_fee)/100 payfee
         from (select distinct * from tmp_o2o_ditui_user) a,
         tmp_o2o_paylog b
         where a.user_id = b.user_id
         and b.pay_mon in ( '''|| p_month ||''')
         group by b.user_id    
                       
         ';
         safely_drop('tmp_o2o_ditui_paylog');
         execute immediate v_sql ;
                
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_ditui_paylog(当月促销员累计缴费数据) OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ; */
                 
         
         
         --- 取当月累计缴费 201811改为 取激活后72小时内充值。
         v_sql := 'create table tmp_o2o_ditui_paylog nologging as
         select distinct user_id,first_value(recv_fee/100) OVER(PARTITION BY user_id ORDER BY recv_time) pay_fee
         from (
         SELECT b.user_id,b.recv_fee,b.recv_time
          from tmp_o2o_ditui_user a,CBSSA_UCR_ACT3.TF_B_PAYLOG_ADD_BIL b
          WHERE a.user_id=b.user_id
          AND b.pay_fee_mode_code<>4
          AND b.payment_op IN (16000,16001)
          and b.recv_fee>0
          AND B.RECV_TIME>=A.DEAL_DT
          AND B.RECV_TIME< A.DEAL_DT+4 )    
                       
         ';
         
         safely_drop('tmp_o2o_ditui_paylog');
         execute immediate v_sql ;
                
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_ditui_paylog(当月促销员累计缴费数据) OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ; 
         
         
            
         ---取当月踢赠出账数据 201811修改规则 不看该指标
         v_sql := 'create table tmp_o2o_ditui_bill nologging as
         select b.user_id,b.cycle_id,sum(b.fee+b.adjust_after-b.writeoff_fee1)/100 fee
         from (select distinct * from tmp_o2o_ditui_user) a,
         yangyi.ts_b_bill_cbss_' || p_month || ' b
         where a.user_id = b.user_id
         and b.cycle_id = ''' || p_month || '''
         group by b.user_id,b.cycle_id 
                       
         ';        
         
         safely_drop('tmp_o2o_ditui_bill');
         execute immediate v_sql ;
                
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_ditui_bill(当月促销员出账数据) OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ; 
         
         
         ---汇总1
         v_sql := 'create table tmp_o2o_ditui_all_tmp1 nologging as
         select a.*,
         nvl(b.pay_fee,0) payfee,
         nvl(c.fee,0) fee,
         nvl(d.status,0) sw_flag,
         1 valid_user,
         nvl(e.status,0) my_flag
         from (select distinct * from tmp_o2o_ditui_user) a,tmp_o2o_ditui_paylog b,
         tmp_o2o_ditui_bill c,tmp_o2o_sw d,tmp_o2o_my e
         where a.user_id=b.user_id(+)
         and a.user_id=c.user_id(+)
         and a.user_id=d.user_id(+) 
         and a.user_id=e.user_id(+)              
         ';     
         
         safely_drop('tmp_o2o_ditui_all_tmp1');
         execute immediate v_sql ;
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_ditui_all_tmp1 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         ---汇总2  201811修改规则 不看出账数据
         --201901修改规则 增加纯漫游用户补计提条件(又不看了)
         v_sql := 'create table tmp_o2o_ditui_all_tmp2 nologging as
         select a.jifen_month,a.staff_name,a.staff_id,a.zhixiao_tag,
a.staff_area,a.staff_salary,a.staff_kpi,a.remark,a.user_id,
a.serial_number,a.cust_id,a.remove_tag,a.user_state_name,
a.deal_dt,a.product_id,a.product_name,a.payfee,a.fee,a.sw_flag,a.valid_user ,
               
         ''o2o地推经理'' as jf_type,
         (case when a.payfee >=50 /*and a.fee >0*/ 
         and a.sw_flag = 0 and /*a.my_flag = 0 and*/ a.user_state_name like ''%正常%'' then 1 
          
         else 0 end) chk_flag ,a.my_flag 
         from tmp_o2o_ditui_all_tmp1 a 
                       
         ';     
         
         safely_drop('tmp_o2o_ditui_all_tmp2');
         execute immediate v_sql ;
         
         --插入到表中做封存数据
         v_sql := 'insert into tmp_o2o_ditui_fengcun
         select distinct a.*
         from tmp_o2o_ditui_all_tmp2 a
         --where a.chk_flag=''1''
                       
         ';        
             
         execute immediate v_sql ;
         commit;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_ditui_all_tmp2 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ; 
         
         
         /*---备份当月不满足条件的号码
         v_sql := 'insert into tmp_o2o_ditui_jfbk 
         select distinct a.*
         from tmp_o2o_ditui_all_tmp2 a
         where a.payfee <50 and jifen_month = ''' || p_month || '''
                       
         '; 
             
         execute immediate v_sql ;
         commit;
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'insert tmp_o2o_ditui_jfbk OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;*/
         
         
         
              
         ---汇总最终积分 201811更新规则 按照首充金额设置提成阶梯 50 100 200档
         --201901更新规则 加入亲情卡计提 
         v_sql := 'create table tmp_o2o_ditui_jifen nologging as
         select distinct a.*,
         (case when  a.payfee >=50 and a.payfee<100 then 30 
         when a.payfee >=100 and a.payfee<200 then 50 
         when a.payfee >=200 then 80
           else 0 end) as jifen_rate
         from tmp_o2o_ditui_all_tmp2 a 
         where chk_flag>=1 and product_name not like ''%亲情卡%''
         union 
         select distinct a.*,
         (case when a.sw_flag = 0 and /*a.my_flag = 0 and */a.user_state_name like ''%正常%'' then 10 
           else 0 end) as jifen_rate
         from tmp_o2o_ditui_all_tmp2 a 
         where product_name  like ''%亲情卡%''        
                       
         ';     
         
         safely_drop('tmp_o2o_ditui_jifen');
         execute immediate v_sql ;
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_ditui_jifen OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ; 
       
         ---算出促销员的当月产能 并取得积分薪酬 201811更新规则 按照首充金额设置提成阶梯
         
         v_sql := 'create table tmp_o2o_ditui_channeng nologging as
         select a.*,nvl(b.cn,0)as cn,
         (case when  b.zhixiao_tag=1 then  nvl(b.jf_rate,0)  else 0 end ) as jf_xinchou
         from tmp_o2o_ditui_base a,
         (
           select 
           a.staff_id,a.zhixiao_tag,sum(a.chk_flag) as cn,sum(a.jifen_rate) as jf_rate,
           ''' || p_month || ''' as cn_month
           from tmp_o2o_ditui_jifen a
           group by a.staff_id,a.zhixiao_tag
         )  b
         where a.staff_id=b.staff_id(+) and a.jifen_month = ''' || p_month || '''
                       
         ';     
         
         safely_drop('tmp_o2o_ditui_channeng');
         execute immediate v_sql ;
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_ditui_channeng OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
 
         ----计算其他薪酬
         v_sql := 'create table tmp_o2o_ditui_xinchou1 nologging as
         select a.*,
         (case when substr(a.jifen_month,-2) between ''05'' and ''10'' then 150 else 0 end) as gw_butie,
         (case when b.cycle_id= a.jifen_month then round(450*nvl(b.rate,0),2) else 0 end) as dt_butie
         from tmp_o2o_ditui_channeng a,(select * from shensw3.t_o2o_saler_months where cycle_id=''' || p_month || ''') b
         where a.staff_id=b.develop_staff_id(+) and a.remark not like ''%督导%'' 
                       
         ';     
         
         safely_drop('tmp_o2o_ditui_xinchou1');
         execute immediate v_sql ;
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_ditui_xinchou1 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         ---计算总薪酬
         
         v_sql := 'create table tmp_o2o_ditui_xinchou nologging as
         select a.*,
         (a.staff_salary+a.jf_xinchou*a.staff_kpi+gw_butie+dt_butie) as cuxiao_xinchou,
         (a.staff_salary|| ''+'' ||a.dt_butie || ''+'' || a.gw_butie||''+''||a.staff_kpi||''*''||a.jf_xinchou ) compulation
         from tmp_o2o_ditui_xinchou1 a  
                       
         ';     
         
         safely_drop('tmp_o2o_ditui_xinchou');
         execute immediate v_sql ;
         
         
         v_sql := 'insert into tmp_o2o_ditui_xinchou_fengcun
         select * from tmp_o2o_ditui_xinchou
         ';
         execute immediate v_sql ;
         commit;
         
                       
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_ditui_xinchou(取得总薪酬) OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
   
   exception 
        when others then     
           v_run_order := v_run_order + 1 ;         
           v_end_flag :=  0;
           v_remark := sqlerrm ;
           insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark);
           commit ;
           
           v_sql := 'delete from tmp_o2o_ditui_xinchou_fengcun
           where jifen_month = ''' || p_month || '''                       
            '; 
             
           execute immediate v_sql ;
           commit;
                    
           v_sql := 'delete from tmp_o2o_ditui_jfbk 
           where jifen_month = ''' || p_month || '''                       
            '; 
             
           execute immediate v_sql ;
           commit;
                     
           v_sql := 'delete from tmp_o2o_ditui_fengcun 
           where jifen_month = ''' || p_month || '''                       
            '; 
             
           execute immediate v_sql ;
           commit;
           
           
   
   
   end p_o2o_ditui_zq ; 
   
   
  ----------------
 --政企渠道-O2O地推督导 
   
   procedure p_o2o_dudao_zq(p_month in varchar2) is
     
         v_start_date date ; 
         v_pkg_name varchar2(50);
         v_prg_name varchar2(50);
         v_run_order number(5);
         v_remark varchar2(2000);
         v_end_flag number(1);         
         v_sql clob;
         v_mon_last varchar2(50);
         v_mon_lastsix varchar2(50);

         --v_count number(2);

     begin

     --log   
         
         v_start_date := sysdate ;
         v_pkg_name := 'pkg_chnl_o2o_data' ;
         v_prg_name := 'p_o2o_dudao_zq' ;
         v_run_order := 0 ;  
         v_mon_last := to_char(add_months(to_date(p_month,'yyyymm'),-1),'yyyymm') ;
         v_mon_lastsix := to_char(add_months(to_date(p_month,'yyyymm'),-6),'yyyymm') ;
        
         IF p_month IS NULL or length(p_month) <> 6 THEN
            raise_application_error(-20001,'p_month is error');
         END IF ;
         
         ---
          v_sql := 'delete from tmp_o2o_dt_xinchou_fengcun 
           where jifen_month = ''' || p_month || '''                       
            '; 
             
           execute immediate v_sql ;
           commit;
           
           v_sql := 'delete from tmp_o2o_dt_hffc_fengcun 
           where p_month = ''' || p_month || '''                       
            '; 
             
           execute immediate v_sql ;
           commit;
           
           v_sql := 'delete from tmp_o2o_dudao_dt_hffc 
           where jifen_month = ''' || p_month || '''                       
            '; 
             
           execute immediate v_sql ;
           commit;
         
         
         
         
         ---取得督导员下属地推经理的直销积分并计算总产能
         
         v_sql := ' create table tmp_o2o_dudao_dt_channeng nologging as
         select x.jifen_month,x.staff_name,x.staff_area,x.staff_id,sum(cn) dudao_cn 
         from ( select a.*,nvl(b.cn,0) as cn
         from tmp_o2o_dudao_dt_relation a,tmp_o2o_ditui_channeng b
         where a.staff_id_b=b.staff_id(+) and b.remark not like ''%督导%'' and a.jifen_month = ''' || p_month || ''' ) x
         group by x.jifen_month,x.staff_name,x.staff_area,x.staff_id     
         ';
         safely_drop('tmp_o2o_dudao_dt_channeng');
         
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_dudao_dt_channeng OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         ---话费分成  取上月开户的user,并把本月数据插入到历史表中
         v_sql := ' create table tmp_o2o_dudao_dt_user nologging as
         select distinct a.*,b.user_id,b.serial_number,b.cust_id,b.remove_tag,
         b.user_state_name,b.deal_dt,b.product_id,b.product_name
         from tmp_o2o_dudao_dt_channeng a,tmp_o2o_ditui_user b,
         tmp_o2o_dudao_dt_relation c
         where a.staff_id=c.staff_id 
         and c.staff_id_b=b.staff_id
         and to_char(b.deal_dt,''yyyymm'') = ''' || p_month || '''     
         ';

         
         safely_drop('tmp_o2o_dudao_dt_user');
         
         execute immediate v_sql ;
         
         v_sql := ' insert into tmp_o2o_dudao_dt_hffc 
         select distinct * from tmp_o2o_dudao_dt_user    
         ';
         
         execute immediate v_sql ;
         commit;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_dudao_dt_user OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         --连续计提6个月 插入历史数据到user表中参与计提
         v_sql := ' insert into tmp_o2o_dudao_dt_user 
         select distinct jifen_month,staff_name,staff_area,staff_id,dudao_cn,
         user_id,serial_number,cust_id,remove_tag,
         user_state_name,deal_dt,product_id,product_name 
         from tmp_o2o_dudao_dt_hffc c
         where c.jifen_month between ''' || v_mon_lastsix || ''' and ''' || v_mon_last || '''    
         ';
         
         execute immediate v_sql ;
         commit;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'insert tmp_o2o_dudao_dt_user OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         ---取出账数据
         
         v_sql := ' create table tmp_o2o_dudao_dt_bill nologging as 
         select b.user_id,b.cycle_id,sum(b.fee+b.adjust_after-b.writeoff_fee1)/100 fee
         from (select distinct * from tmp_o2o_dudao_dt_user) a,yangyi.ts_b_bill_cbss_' || p_month || ' b
         where a.user_id = b.user_id
         and b.cycle_id = ''' || p_month || '''
         and to_char(a.deal_dt,''yyyymm'') between ''' || v_mon_lastsix || ''' and ''' || v_mon_last || '''         group by b.user_id,b.cycle_id    
         ';
         
         safely_drop('tmp_o2o_dudao_dt_bill');
         execute immediate v_sql ;
        
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_dudao_dt_bill OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         ---取三无指标
         
         v_sql := ' create table tmp_o2o_dudao_dt_sw nologging as 
         select a.user_id,nvl(b.status,0) status
         from (select distinct * from tmp_o2o_dudao_dt_user) a,tmp_o2o_sw b
         where a.user_id=b.user_id(+)    
         ';
         
         
         safely_drop('tmp_o2o_dudao_dt_sw');         
         execute immediate v_sql ;
              
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_dudao_dt_sw OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         --得到当月的话费分成
         v_sql := ' create table tmp_o2o_dudao_dt_fencheng nologging as 
         select staff_id,staff_name,sum(fee) hf_fencheng 
         from (select a.*,round(b.fee*0.01,1) fee 
         from (select distinct * from tmp_o2o_dudao_dt_user
         where to_char(deal_dt,''yyyymm'') between ''' || v_mon_lastsix || ''' and ''' || v_mon_last || ''') a,tmp_o2o_dudao_dt_bill b,tmp_o2o_dudao_dt_sw c
         where a.user_id=b.user_id
         and a.user_id=c.user_id
         and b.fee>0 and c.status=0 )  
         group by staff_id,staff_name   
         ';
         
         safely_drop('tmp_o2o_dudao_dt_fencheng');         
         execute immediate v_sql ;
              
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_dudao_dt_fencheng OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         

         ---把话费分成明细放到封存表
         v_sql:= ' insert into tmp_o2o_dt_hffc_fengcun 
         select a.*, fee,''' || p_month || ''' p_month 
         from (select distinct * from tmp_o2o_dudao_dt_user
         where to_char(deal_dt,''yyyymm'') between ''' || v_mon_lastsix || ''' and ''' || v_mon_last || ''') a,
         tmp_o2o_dudao_dt_bill b,tmp_o2o_dudao_dt_sw c
         where a.user_id=b.user_id
         and a.user_id=c.user_id
         and b.fee>0 and c.status=0    
         ';
         
         execute immediate v_sql ;
         commit;
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'insert tmp_o2o_dt_hffc_fengcun OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
  
         
         ---汇总督导员薪酬
         v_sql := ' create table tmp_o2o_dudao_dt_xinchou1 nologging as 
         select b.* ,nvl(c.hf_fencheng,0) as hf_fencheng,nvl(d.dudao_cn,0) dudao_cn
         from (select * from tmp_o2o_ditui_channeng where remark like ''%督导%'') b,
         tmp_o2o_dudao_dt_fencheng c,tmp_o2o_dudao_dt_channeng d
         where  b.staff_id=c.staff_id(+) and b.staff_id=d.staff_id(+)   
         ';
         
         safely_drop('tmp_o2o_dudao_dt_xinchou1');         
         execute immediate v_sql ;
              
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_dudao_dt_xinchou1 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         ---计算最终薪酬
         v_sql := ' create table tmp_o2o_dudao_dt_xinchou nologging as 
         select a.*,
         (a.staff_salary + a.jf_xinchou*a.staff_kpi + a.hf_fencheng*a.staff_kpi +
          a.dudao_cn*4*a.staff_kpi) as dudao_xinchou,
         (a.staff_salary|| ''+'' ||a.staff_kpi||''*''||a.jf_xinchou || ''+'' ||a.staff_kpi||''*''||a.hf_fencheng || ''+'' ||a.staff_kpi||''*''||a.dudao_cn|| ''*''||''4'' ) compulation
          from tmp_o2o_dudao_dt_xinchou1 a   
         ';
         
         safely_drop('tmp_o2o_dudao_dt_xinchou');         
         execute immediate v_sql ;
         
         
         v_sql:= 'insert into tmp_o2o_dt_xinchou_fengcun 
         select * from tmp_o2o_dudao_dt_xinchou  
         ';
         execute immediate v_sql ;
         commit;
         
                     
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_dudao_dt_xinchou OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
                    
      exception 
        when others then     
           v_run_order := v_run_order + 1 ;         
           v_end_flag :=  0;
           v_remark := sqlerrm ;
           insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark);
           commit ;
           
           v_sql := 'delete from tmp_o2o_dt_xinchou_fengcun 
           where jifen_month = ''' || p_month || '''                       
            '; 
             
           execute immediate v_sql ;
           commit;
           
           v_sql := 'delete from tmp_o2o_dt_hffc_fengcun 
           where p_month = ''' || p_month || '''                       
            '; 
             
           execute immediate v_sql ;
           commit;
           
           v_sql := 'delete from tmp_o2o_dudao_dt_hffc 
           where jifen_month = ''' || p_month || '''                       
            '; 
             
           execute immediate v_sql ;
           commit;
           
           
           
   end p_o2o_dudao_zq ;
   
   
 ----------------
 --政企渠道-O2O王卡兼职生
   
   procedure p_o2o_jianzhi_zq(p_month in varchar2) is
     
         v_start_date date ; 
         v_pkg_name varchar2(50);
         v_prg_name varchar2(50);
         v_run_order number(5);
         v_remark varchar2(2000);
         v_end_flag number(1);         
         v_sql clob;
         --v_mon_last varchar2(50);
         --v_mon_lastsix varchar2(50);

         --v_count number(2);

     begin

     --log   
         
         v_start_date := sysdate ;
         v_pkg_name := 'pkg_chnl_o2o_data' ;
         v_prg_name := 'p_o2o_jianzhi_zq' ;
         v_run_order := 0 ;  
         --v_mon_last := to_char(add_months(to_date(p_month,'yyyymm'),-1),'yyyymm') ;
         --v_mon_lastsix := to_char(add_months(to_date(p_month,'yyyymm'),-5),'yyyymm') ;
        
         IF p_month IS NULL or length(p_month) <> 6 THEN
            raise_application_error(-20001,'p_month is error');
         END IF ;
         
         
         --
         v_sql := 'delete from tmp_o2o_jz_xinchou_fengcun 
           where jifen_month = ''' || p_month || '''                       
            '; 
                      
           execute immediate v_sql ;
           commit;
           
           v_sql := 'delete from tmp_o2o_jianzhi_jfbk 
           where jifen_month = ''' || p_month || '''                       
            '; 
                      
           execute immediate v_sql ;
           commit;
           
           
           v_sql := 'delete from tmp_o2o_jianzhi_fengcun 
           where jifen_month = ''' || p_month || '''                       
            '; 
                      
           execute immediate v_sql ;
           commit;
         
         
         ---当月发展用户 201901修改 不看90350781 90373960 亲情卡产品
         
         v_sql := ' create table tmp_o2o_jianzhi_user nologging as
         select * from (
         select a.jifen_month,a.staff_name,a.staff_id,
         a.staff_area,a.chuqin_day,a.remark,b.user_id,b.serial_number
         ,b.cust_id,b.remove_tag,b.user_state_name,b.deal_dt,b.product_id,b.product_name
         from tmp_o2o_jianzhi_base a,report.t_user_all_'|| p_month ||' b
         where a.staff_id=b.cbss_developer_id and a.jifen_month = ''' || p_month || ''')
               where to_char(deal_dt,''yyyymm'') = ''' || p_month || ''' and product_name not like ''%亲情卡%''        
         ';
         safely_drop('tmp_o2o_jianzhi_user');
         
         
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_jianzhi_user(当月促销员发展用户数据) OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ; 
         
         
         ----计算当月充值数据 201811修改规则 72小时首充>=50
         
        /* v_sql := ' create table tmp_o2o_jianzhi_paylog nologging as
         select a.jifen_month,a.staff_name,a.staff_id,a.staff_area,
              a.chuqin_day,a.remark,a.user_id,a.serial_number,a.cust_id,
              a.remove_tag,a.user_state_name,a.deal_dt,a.product_id,
              a.product_name,sum(b.recv_fee)/100 payfee
         from tmp_o2o_jianzhi_user a,tmp_o2o_paylog b
         where a.user_id = b.user_id
         and b.pay_mon in ( ''' || p_month || ''')
         group by a.jifen_month,a.staff_name,a.staff_id,a.staff_area,
              a.chuqin_day,a.remark,a.user_id,a.serial_number,a.cust_id,
              a.remove_tag,a.user_state_name,a.deal_dt,a.product_id,
              a.product_name  having(sum(b.recv_fee)/100)>=50         
         ';
         safely_drop('tmp_o2o_jianzhi_paylog');
         
         
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_jianzhi_paylog(当月促销员充值数据) OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;*/
         
          v_sql := ' create table tmp_o2o_jianzhi_paylog nologging as
        select c.jifen_month,c.staff_name,c.staff_id,c.staff_area,
              c.chuqin_day,c.remark,c.user_id,c.serial_number,c.cust_id,
              c.remove_tag,c.user_state_name,c.deal_dt,c.product_id,
              c.product_name,d.pay_fee 
         from 
         tmp_o2o_jianzhi_user c,(
         select distinct user_id,first_value(recv_fee/100) OVER(PARTITION BY user_id ORDER BY recv_time) pay_fee
         from (
         SELECT b.user_id,b.recv_fee,b.recv_time
          from tmp_o2o_jianzhi_user a,CBSSA_UCR_ACT3.TF_B_PAYLOG_ADD_BIL b
          WHERE a.user_id=b.user_id
          AND b.pay_fee_mode_code<>4
          AND b.payment_op IN (16000,16001)
          and b.recv_fee>0
          AND B.RECV_TIME>=A.DEAL_DT
          AND B.RECV_TIME< A.DEAL_DT+4 )
          )  d   
          where c.user_id = d.user_id
          and d.pay_fee>=50         
         ';
         safely_drop('tmp_o2o_jianzhi_paylog');
         
        
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_jianzhi_paylog(当月促销员充值数据) OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
             
         
         ---汇总数据
        
         v_sql := ' create table tmp_o2o_jianzhi_charge nologging as
         select jifen_month ,staff_name ,staff_id ,
         staff_area ,chuqin_day ,remark ,count(1) as charge 
         from tmp_o2o_jianzhi_paylog a
         group by jifen_month ,staff_name ,staff_id ,
         staff_area ,chuqin_day ,remark         
         ';
         
         safely_drop('tmp_o2o_jianzhi_charge');
         
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_jianzhi_charge  OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         ---计算底薪
         
         v_sql := ' create table tmp_o2o_jianzhi_dixin nologging as
         select a.*,(case when a.chuqin_day<=floor(a.charge/3) then a.chuqin_day*100 
         else floor(a.charge/3)*100 end ) as dixin,
        (case when a.chuqin_day<=floor(a.charge/3) then 1 else 2 end ) charge_tag  
         from tmp_o2o_jianzhi_charge a         
         ';
         
         safely_drop('tmp_o2o_jianzhi_dixin');
         
         execute immediate v_sql ;
                 
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_jianzhi_dixin OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         /*---取上月不满足条件的用户 本月继续观察 201811修改 不看该指标
         
         v_sql := ' insert into tmp_o2o_jianzhi_user
         select a.jifen_month,a.staff_name,
         a.staff_id,a.staff_area,a.chuqin_day,a.remark,a.user_id,serial_number,cust_id,remove_tag,
         user_state_name,deal_dt,product_id,product_name
         from tmp_o2o_jianzhi_jfbk a
         where jifen_month=''' || v_mon_last || ''' 
                 
         ';
                 
         execute immediate v_sql ;
         commit;
                 
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'insert tmp_o2o_jianzhi_user OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;*/
         
         
         /*--取累计缴费 201811改为修改看72小时内首充
         
         v_sql := ' create table tmp_o2o_jianzhi_paylog1 nologging as
         select b.user_id,sum(b.recv_fee)/100 payfee
         from tmp_o2o_jianzhi_user a,
         tmp_o2o_paylog b
         where a.user_id = b.user_id
         and b.pay_mon in ( ''' || p_month || ''')
         group by b.user_id  
                 
         ';
         
         safely_drop('tmp_o2o_jianzhi_paylog1');       
         execute immediate v_sql ;
         
                 
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'insert tmp_o2o_jianzhi_user OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;*/
         
         --取累计缴费 201811改为修改看72小时内首充
         
         v_sql := ' create table tmp_o2o_jianzhi_paylog1 nologging as
        select distinct user_id,first_value(recv_fee/100) OVER(PARTITION BY user_id ORDER BY recv_time) pay_fee
         from (
         SELECT b.user_id,b.recv_fee,b.recv_time
          from tmp_o2o_jianzhi_user a,CBSSA_UCR_ACT3.TF_B_PAYLOG_ADD_BIL b
          WHERE a.user_id=b.user_id
          AND b.pay_fee_mode_code<>4
          AND b.payment_op IN (16000,16001)
          and b.recv_fee>0
          AND B.RECV_TIME>=A.DEAL_DT
          AND B.RECV_TIME< A.DEAL_DT+4 )  
                 
         ';
                
         
         safely_drop('tmp_o2o_jianzhi_paylog1');       
         execute immediate v_sql ;
         
                 
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'insert tmp_o2o_jianzhi_user OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         
         
         ---取当月踢赠出账数据 201811修改规则 不看该指标
         v_sql := 'create table tmp_o2o_jianzhi_bill nologging as
         select b.user_id,b.cycle_id,sum(b.fee+b.adjust_after-b.writeoff_fee1)/100 fee
         from (select distinct * from tmp_o2o_jianzhi_user) a,
         yangyi.ts_b_bill_cbss_' || p_month || ' b
         where a.user_id = b.user_id
         and b.cycle_id = ''' || p_month || '''
         group by b.user_id,b.cycle_id 
                       
         ';        
         
         safely_drop('tmp_o2o_jianzhi_bill');
         execute immediate v_sql ;
                
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_jianzhi_bill(当月促销员出账数据) OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ; 
         
         ---汇总1
         
         v_sql := 'create table tmp_o2o_jianzhi_all_tmp nologging as
         select a.*,nvl(b.pay_fee,0) payfee,nvl(c.fee,0) fee,nvl(d.status,0) sw_flag,1 valid_user
         from tmp_o2o_jianzhi_user a,tmp_o2o_jianzhi_paylog1 b,
         tmp_o2o_jianzhi_bill c,tmp_o2o_sw d
         where a.user_id=b.user_id(+) and a.user_id=c.user_id(+) and a.user_id=d.user_id(+) 
                       
         ';        
         
         safely_drop('tmp_o2o_jianzhi_all_tmp');
         execute immediate v_sql ;
                
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_jianzhi_all_tmp OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         ---汇总2并保持到封存表 201811修改不看出账
         
         v_sql := 'create table tmp_o2o_jianzhi_all nologging as
         select a.*,''o2o王卡兼职生'' as jf_type,
         (case when a.payfee >=50 /*and a.fee >0 */
         and a.sw_flag = 0 and a.user_state_name like ''%正常%'' then 1 else 0 end) chk_flag  
         from tmp_o2o_jianzhi_all_tmp a 
                       
         ';        
         
         safely_drop('tmp_o2o_jianzhi_all');
         execute immediate v_sql ;
         
         
         
         
         v_sql := 'insert into tmp_o2o_jianzhi_fengcun
         select a.*
         from tmp_o2o_jianzhi_all a
         
         ';
         execute immediate v_sql ;
         commit;
                
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_jianzhi_all OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         ---汇总最终满足条件积分
         
         v_sql := 'create table tmp_o2o_jianzhi_jifen nologging as
         select distinct a.*,(case when  a.payfee >=50 and a.payfee<100 then 30 
         when a.payfee >=100 and a.payfee<200 then 50 
         when a.payfee >=200 then 80
           else 0 end) as jifen_rate 
         from tmp_o2o_jianzhi_all a 
         where chk_flag=1                       
         ';  
         
                   
         
         safely_drop('tmp_o2o_jianzhi_jifen');
         execute immediate v_sql ;
                
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_jianzhi_jifen OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         ---备份当月不满足条件的号码
         
         v_sql := 'insert into tmp_o2o_jianzhi_jfbk 
         select distinct a.*
         from tmp_o2o_jianzhi_all a
         where a.payfee <50 and jifen_month = ''' || p_month || '''
                       
         '; 
             
         execute immediate v_sql ;
         commit;
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'insert tmp_o2o_jianzhi_jfbk OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         ---算出促销员的当月产能 并取得积分薪酬
         
         v_sql := 'create table tmp_o2o_jianzhi_channeng nologging as
         select a.*,nvl(b.cn,0)as cn,nvl(b.jifen_rate,0) as jf_xinchou
         from tmp_o2o_jianzhi_base a,
         (select a.staff_id,sum(a.chk_flag) cn,sum(a.jifen_rate) as jifen_rate,''' || p_month || ''' as cn_month
         from tmp_o2o_jianzhi_jifen a
         group by a.staff_id) b
         where a.staff_id=b.staff_id(+) and jifen_month = ''' || p_month || '''    
                       
         '; 
         
                     
         safely_drop('tmp_o2o_jianzhi_channeng');
         execute immediate v_sql ;
                
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_jianzhi_channeng OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         ---汇总底薪加提成
         
         v_sql := 'create table tmp_o2o_jianzhi_xinchou nologging as
         select a.* ,nvl(b.charge,0) charge,nvl(b.dixin,0) dixin,nvl(b.charge_tag,0) charge_tag,
         (a.jf_xinchou+nvl(b.dixin,0)) as xinchou
         from tmp_o2o_jianzhi_channeng a,tmp_o2o_jianzhi_dixin b
         where a.staff_id=b.staff_id(+) 
                       
         ';        
         
         safely_drop('tmp_o2o_jianzhi_xinchou');
         execute immediate v_sql ;
         
          v_sql:= 'insert into tmp_o2o_jz_xinchou_fengcun 
         select * from tmp_o2o_jianzhi_xinchou  
         ';
         execute immediate v_sql ;
         commit;
                
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_jianzhi_xinchou OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
                         
       exception 
        when others then     
           v_run_order := v_run_order + 1 ;         
           v_end_flag :=  0;
           v_remark := sqlerrm ;
           insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark);
           commit ;
           
           v_sql := 'delete from tmp_o2o_jz_xinchou_fengcun 
           where jifen_month = ''' || p_month || '''                       
            '; 
                      
           execute immediate v_sql ;
           commit;
           
           v_sql := 'delete from tmp_o2o_jianzhi_jfbk 
           where jifen_month = ''' || p_month || '''                       
            '; 
                      
           execute immediate v_sql ;
           commit;
           
           
           v_sql := 'delete from tmp_o2o_jianzhi_fengcun 
           where jifen_month = ''' || p_month || '''                       
            '; 
                      
           execute immediate v_sql ;
           commit;
                   
          
    end p_o2o_jianzhi_zq ;
    
    
    procedure p_o2o_dashi_yy(p_month in varchar2) is
      
         v_start_date date ; 
         v_pkg_name varchar2(50);
         v_prg_name varchar2(50);
         v_run_order number(5);
         v_remark varchar2(300);
         v_end_flag number(1);
         v_sql clob;
         
       begin
         
         v_start_date := sysdate ;
         v_pkg_name := 'pkg_chnl_o2o_data' ;
         v_prg_name := 'p_o2o_dashi_yy' ;
         v_run_order := 0 ;  
        
         IF p_month IS  NULL or length(p_month) <> 6 THEN
            raise_application_error(-20001,'p_month is error');
         END IF ;
         
         
        /* safely_drop('TMP_o2o_HHB_XSTC_base');
         
         v_sql := 'create table TMP_o2o_HHB_XSTC_base nologging as
         select * from (
         select a.*,b.serial_number device_number,b.deal_dt
         from TMP_o2o_HHB_XSTC_base1 a,report.t_user_all_'|| p_month ||' b
         where a.cbss_developer_id=b.cbss_developer_id
         and a.jifen_month = ''' || p_month || '''
         and  to_char(deal_dt,''yyyymm'') = ''' || p_month || ''')  where            
         '; */
         
         
         
         safely_drop('TMP_o2o_HHB_XSTC_2');
         
         v_sql := 'create table TMP_o2o_HHB_XSTC_2 nologging as    
         SELECT A.*,B.USER_ID,B.DEAL_DT,B.USER_STATE_NAME
         FROM TMP_o2o_HHB_XSTC_base1 A,REPORT.T_USER_ALL_'|| p_month ||' B
         WHERE A.DEVICE_NUMBER=B.SERIAL_NUMBER
         AND B.REMOVE_TAG=''0'' AND B.BUSITYPE=4
         AND a.jifen_month = ''' || p_month || '''            
         ';   
         
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create TMP_o2o_HHB_XSTC_2 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         safely_drop('TMP_o2o_HHB_XSTC_2_tmp');
         
         v_sql := 'create table TMP_o2o_HHB_XSTC_2_tmp nologging as    
         SELECT a.*,nvl(b.status,0) sw_flag
         FROM TMP_o2o_HHB_XSTC_2 A,tmp_o2o_sw B
         WHERE A.user_id=B.user_id(+)
                    
         ';   
         
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create TMP_o2o_HHB_XSTC_2_tmp OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         
         /*201902月分1-12号，13到28日充值口径不同*/
         IF p_month <>'201902' THEN
           
             safely_drop('TMP_o2o_HHB_XSTC_3');
             
             v_sql := 'create table TMP_o2o_HHB_XSTC_3 nologging as
              SELECT a.*,b.recv_fee,b.recv_time,b.recv_staff_id
              from TMP_o2o_HHB_XSTC_2 a,CBSSA_UCR_ACT3.TF_B_PAYLOG_ADD_BIL b
              WHERE a.user_id=b.user_id
              AND b.pay_fee_mode_code<>4
              AND b.payment_op IN (16000,16001)
              and b.recv_fee>0
              AND B.RECV_TIME>=A.DEAL_DT
              AND B.RECV_TIME< A.DEAL_DT+2             
             ';   
             
             execute immediate v_sql ;
             
             
             v_run_order := v_run_order + 1 ;
             v_end_flag := 1 ;
             v_remark := 'create TMP_o2o_HHB_XSTC_3 OK  ';
             insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
             commit ;
             
             
             safely_drop('TMP_o2o_HHB_XSTC_4');
             
             v_sql := 'create table TMP_o2o_HHB_XSTC_4 nologging as
              SELECT DISTINCT a.user_id,
              first_value(a.recv_fee) OVER(PARTITION BY user_id ORDER BY a.recv_time) fisrt_fee,
              first_value(a.recv_time) OVER(PARTITION BY user_id ORDER BY a.recv_time) fisrt_recv_time,
              SUM(a.recv_fee) OVER(PARTITION BY user_id) sum_fee,
              first_value(a.recv_fee) OVER(PARTITION BY user_id ORDER BY a.recv_time DESC) last_fee,
              first_value(a.recv_time) OVER(PARTITION BY user_id ORDER BY a.recv_time DESC) last_recv_time,
              SUM(CASE WHEN trunc(a.recv_time)-TRUNC(a.deal_dt)<=2 THEN a.recv_fee ELSE 0 END) OVER(PARTITION BY user_id) fee_in_5day,
              SUM(CASE WHEN TRUNC(a.recv_time,''MM'')=TRUNC(SYSDATE,''MM'') THEN a.recv_fee ELSE 0 END) OVER(PARTITION BY user_id) fee_month
              from TMP_o2o_HHB_XSTC_3 a             
             ';   
             
             execute immediate v_sql ;
             
             
             v_run_order := v_run_order + 1 ;
             v_end_flag := 1 ;
             v_remark := 'create TMP_o2o_HHB_XSTC_4 OK  ';
             insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
             commit ;
             
             
             safely_drop('TMP_o2o_HHB_XSTC_5');
             
             v_sql := 'create table TMP_o2o_HHB_XSTC_5 nologging as
              SELECT A.*,B.FISRT_FEE/100 FIRST_FEE,c.sw_flag
              FROM TMP_o2o_HHB_XSTC_2 A,TMP_o2o_HHB_XSTC_4 B,TMP_o2o_HHB_XSTC_2_tmp c
              WHERE A.USER_ID=B.USER_ID(+) and A.USER_ID=c.USER_ID(+)            
             ';   
             
             execute immediate v_sql ;
             
             
             v_run_order := v_run_order + 1 ;
             v_end_flag := 1 ;
             v_remark := 'create TMP_o2o_HHB_XSTC_5 OK  ';
             insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
             commit ;
              
         
         else 
           
             safely_drop('TMP_o2o_HHB_XSTC_3');
             
             v_sql := 'create table TMP_o2o_HHB_XSTC_3 nologging as
              SELECT a.*,b.recv_fee,b.recv_time,b.recv_staff_id
              from TMP_o2o_HHB_XSTC_2 a,CBSSA_UCR_ACT3.TF_B_PAYLOG_ADD_BIL b
              WHERE a.user_id=b.user_id
              AND b.pay_fee_mode_code<>4
              AND b.payment_op IN (16000,16001)
              and b.recv_fee>0
              AND B.RECV_TIME>=A.DEAL_DT
              AND B.RECV_TIME< A.DEAL_DT+2
              and to_char(a.deal_dt,''yyyymmdd'')>=''20190213'' 
              union all
              SELECT a.*,b.recv_fee,b.recv_time,b.recv_staff_id
              from TMP_o2o_HHB_XSTC_2 a,CBSSA_UCR_ACT3.TF_B_PAYLOG_ADD_BIL b
              WHERE a.user_id=b.user_id
              AND b.pay_fee_mode_code<>4
              AND b.payment_op IN (16000,16001)
              and b.recv_fee>0
              AND B.RECV_TIME>=A.DEAL_DT
              AND B.RECV_TIME< A.DEAL_DT+6
              and to_char(a.deal_dt,''yyyymmdd'')<''20190213'' 
                            
             ';   
             
             execute immediate v_sql ;
             
             
             v_run_order := v_run_order + 1 ;
             v_end_flag := 1 ;
             v_remark := 'create TMP_o2o_HHB_XSTC_3 OK  ';
             insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
             commit ;
             
             
             safely_drop('TMP_o2o_HHB_XSTC_4');
             
             v_sql := 'create table TMP_o2o_HHB_XSTC_4 nologging as
              SELECT DISTINCT a.user_id,
              first_value(a.recv_fee) OVER(PARTITION BY user_id ORDER BY a.recv_time) fisrt_fee,
              first_value(a.recv_time) OVER(PARTITION BY user_id ORDER BY a.recv_time) fisrt_recv_time,
              SUM(a.recv_fee) OVER(PARTITION BY user_id) sum_fee,
              first_value(a.recv_fee) OVER(PARTITION BY user_id ORDER BY a.recv_time DESC) last_fee,
              first_value(a.recv_time) OVER(PARTITION BY user_id ORDER BY a.recv_time DESC) last_recv_time,
              SUM(CASE WHEN trunc(a.recv_time)-TRUNC(a.deal_dt)<=2 THEN a.recv_fee ELSE 0 END) OVER(PARTITION BY user_id) fee_in_5day,
              SUM(CASE WHEN TRUNC(a.recv_time,''MM'')=TRUNC(SYSDATE,''MM'') THEN a.recv_fee ELSE 0 END) OVER(PARTITION BY user_id) fee_month
              from TMP_o2o_HHB_XSTC_3 a 
              where to_char(a.deal_dt,''yyyymmdd'') >= ''20190213''
              union 
              SELECT DISTINCT a.user_id,
              SUM(CASE WHEN trunc(a.recv_time)-TRUNC(a.deal_dt)<=6 THEN a.recv_fee ELSE 0 END) OVER(PARTITION BY user_id) fisrt_fee,
              first_value(a.recv_time) OVER(PARTITION BY user_id ORDER BY a.recv_time) fisrt_recv_time,
              SUM(a.recv_fee) OVER(PARTITION BY user_id) sum_fee,
              first_value(a.recv_fee) OVER(PARTITION BY user_id ORDER BY a.recv_time DESC) last_fee,
              first_value(a.recv_time) OVER(PARTITION BY user_id ORDER BY a.recv_time DESC) last_recv_time,
              SUM(CASE WHEN trunc(a.recv_time)-TRUNC(a.deal_dt)<=6 THEN a.recv_fee ELSE 0 END) OVER(PARTITION BY user_id) fee_in_5day,
              SUM(CASE WHEN TRUNC(a.recv_time,''MM'')=TRUNC(SYSDATE,''MM'') THEN a.recv_fee ELSE 0 END) OVER(PARTITION BY user_id) fee_month
              from TMP_o2o_HHB_XSTC_3 a 
              where to_char(a.deal_dt,''yyyymmdd'') < ''20190213''
                          
             ';   
             
             execute immediate v_sql ;
             
             
             v_run_order := v_run_order + 1 ;
             v_end_flag := 1 ;
             v_remark := 'create TMP_o2o_HHB_XSTC_4 OK  ';
             insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
             commit ;
             
             
             safely_drop('TMP_o2o_HHB_XSTC_5');
             
             v_sql := 'create table TMP_o2o_HHB_XSTC_5 nologging as
              SELECT A.*,B.FISRT_FEE/100 FIRST_FEE,c.sw_flag
              FROM TMP_o2o_HHB_XSTC_2 A,TMP_o2o_HHB_XSTC_4 B,TMP_o2o_HHB_XSTC_2_tmp c
              WHERE A.USER_ID=B.USER_ID(+) and A.USER_ID=c.USER_ID(+)            
             ';   
             
             execute immediate v_sql ;
             
             
             v_run_order := v_run_order + 1 ;
             v_end_flag := 1 ;
             v_remark := 'create TMP_o2o_HHB_XSTC_5 OK  ';
             insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
             commit ;
           
             
         
         
         END IF ;
          
              
         
          safely_drop('TMP_o2o_HHB_XSTC_6');
         
         v_sql := 'create table TMP_o2o_HHB_XSTC_6 nologging as
          SELECT A.DEVICE_NUMBER,A.DEVELOP_STAFF_ID,A.BUSI_TYPE,A.USER_TYPE,
A.DEAL_DT,A.FIRST_FEE,a.sw_flag,
CASE WHEN A.USER_TYPE IN (''营业人员'',''王卡大使'') THEN 10
     WHEN A.USER_TYPE=''兼职生''  THEN 18
     WHEN A.USER_TYPE=''实习生''  THEN 18
     ELSE 0
     END JH_PRIZE,
CASE WHEN A.USER_TYPE IS NULL THEN 0
     WHEN A.BUSI_TYPE=''自提'' AND A.USER_TYPE=''营业人员''
       THEN (CASE WHEN A.FIRST_FEE>=50 AND A.FIRST_FEE<100 THEN 5
                  WHEN A.FIRST_FEE>=100 AND A.FIRST_FEE<200 THEN 25
                  WHEN A.FIRST_FEE>=200 THEN 55
                  ELSE 0 END)
     ELSE (CASE WHEN A.FIRST_FEE>=50 AND A.FIRST_FEE<100 THEN 20
                  WHEN A.FIRST_FEE>=100 AND A.FIRST_FEE<200 THEN 40
                  WHEN A.FIRST_FEE>=200 THEN 70
                  ELSE 0 END)
     END CZ_PRIZE,
A.USER_STATE_NAME
FROM TMP_o2o_HHB_XSTC_5 A            
         ';   
         
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create TMP_o2o_HHB_XSTC_6 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
          safely_drop('TMP_o2o_HHB_XSTC_7');
         
         v_sql := 'create table TMP_o2o_HHB_XSTC_7 nologging as
          SELECT A.DEVICE_NUMBER 订购号码,
           A.DEVELOP_STAFF_ID 发展人编码,
           A.BUSI_TYPE 类型,
           A.USER_TYPE 人员类型,
           A.DEAL_DT 开户时间,
           A.FIRST_FEE 七十二小时内首次充值金额,
           A.JH_PRIZE 激活激励,
           A.CZ_PRIZE 充值激励,
           A.JH_PRIZE + A.CZ_PRIZE 激励合计,
           A.USER_STATE_NAME 当月月末状态,
           a.sw_flag 是否三无
          FROM TMP_o2o_HHB_XSTC_6 A          
         ';   
         
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create TMP_o2o_HHB_XSTC_7 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         --加入封存表
          v_sql := 'delete from TMP_o2o_HHB_XSTC_fengcun 
           where jifen_month = ''' || p_month || '''                       
            '; 
                      
           execute immediate v_sql ;
           commit;
         
         v_sql := 'insert into  TMP_o2o_HHB_XSTC_fengcun 
         select ''' || p_month || ''' as jifen_month,a.*
         from TMP_o2o_HHB_XSTC_7 a' ;
         
         execute immediate v_sql ;
         commit;
         
         
         
         
        exception 
        when others then     
           v_run_order := v_run_order + 1 ;         
           v_end_flag :=  0;
           v_remark := sqlerrm ;
           insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark);
           commit ;
           
           v_sql := 'delete from TMP_o2o_HHB_XSTC_fengcun 
           where jifen_month = ''' || p_month || '''                       
            '; 
                      
           execute immediate v_sql ;
           commit;
           
              
    end p_o2o_dashi_yy;
    
    
    
    procedure p_o2o_qinqingka_yy(p_month in varchar2) is
      
         v_start_date date ; 
         v_pkg_name varchar2(50);
         v_prg_name varchar2(50);
         v_run_order number(5);
         v_remark varchar2(300);
         v_end_flag number(1);
         v_sql clob;
         
       begin
         
         v_start_date := sysdate ;
         v_pkg_name := 'pkg_chnl_o2o_data' ;
         v_prg_name := 'p_o2o_qinqingka_yy' ;
         v_run_order := 0 ;  
        
         IF p_month IS  NULL or length(p_month) <> 6 THEN
            raise_application_error(-20001,'p_month is error');
         END IF ;
         
         
         safely_drop('TMP_o2o_HHB_QQK_base');
         
         v_sql := 'create table TMP_o2o_HHB_QQK_base nologging as
         SELECT serial_number DEVICE_NUMBER
        FROM tmp_o2o_wangka_fengcun a
        WHERE /*a.depart_type=''营业线'' and*/ a.jifen_month= '''|| p_month ||'''
        and a.bss_product_id in (''90350781'',''90373960'')          
         ';   
         
         execute immediate v_sql ;
         
         
         safely_drop('TMP_o2o_HHB_QQK_UU');
         
         v_sql := 'create table TMP_o2o_HHB_QQK_UU nologging as
         SELECT SERIAL_NUMBER_A,
        SERIAL_NUMBER_B,
         USER_ID_B,
         ROLE_CODE_B,
         START_DATE,
         END_DATE,
         ITEM_ID
        FROM CBSSC_UCR_CRM3.TF_F_RELATION_UU@LINK_4GBSS_all
        WHERE RELATION_TYPE_CODE = ''ZF''             
         ';   
         
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create TMP_o2o_HHB_QQK_UU OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         safely_drop('TMP_o2o_HHB_QQK_2');
         
         v_sql := 'create table TMP_o2o_HHB_QQK_2 nologging as
         SELECT A.DEVICE_NUMBER,B.USER_ID,B.USER_STATE_NAME
          FROM TMP_o2o_HHB_QQK_base A,REPORT.T_USER_ALL_'|| p_month ||' B
          WHERE A.DEVICE_NUMBER=B.SERIAL_NUMBER
          AND B.REMOVE_TAG=''0''            
         ';   
         
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create TMP_o2o_HHB_QQK_2 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         
         safely_drop('TMP_o2o_HHB_QQK_3');
         
         v_sql := 'create table TMP_o2o_HHB_QQK_3 nologging as
         SELECT DISTINCT A.SERIAL_NUMBER_A
          FROM TMP_o2o_HHB_QQK_UU A,TMP_o2o_HHB_QQK_2 B
         WHERE A.USER_ID_B = B.USER_ID            
         ';   
         
         execute immediate v_sql ;
         
         
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create TMP_FZQ_HHB_QQK_3 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         safely_drop('TMP_o2o_HHB_QQK_4');
         
         v_sql := 'create table TMP_o2o_HHB_QQK_4 nologging as
         SELECT a.serial_number_a,serial_number_b,user_id_b,a.start_date,a.end_date,
          ROW_number() OVER(PARTITION BY a.serial_number_a ORDER BY a.item_id) ROW_num
            FROM TMP_o2o_HHB_QQK_UU a,TMP_o2o_HHB_QQK_3 b
          WHERE a.serial_number_a=b.serial_number_a AND a.role_code_b=''0''            
         ';   
         
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create TMP_FZQ_HHB_QQK_4 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         
         safely_drop('TMP_o2o_HHB_QQK_5');
         
         v_sql := 'create table TMP_o2o_HHB_QQK_5 nologging as
         SELECT * FROM TMP_o2o_HHB_QQK_4 WHERE row_num=1            
         ';   
         
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create TMP_o2o_HHB_QQK_5 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         safely_drop('TMP_o2o_HHB_QQK_6');
         
         v_sql := 'create table TMP_o2o_HHB_QQK_6 nologging as
         SELECT A.DEVICE_NUMBER 电话号码,
       A.USER_STATE_NAME 月末状态,
       B.SERIAL_NUMBER_A 主卡号码,
       C.SERIAL_NUMBER_B 是否第一张亲情卡,
       CASE
         WHEN A.USER_STATE_NAME = ''正常开机'' AND C.SERIAL_NUMBER_B IS NOT NULL THEN
          ''是''
         ELSE
          ''否''
       END 是否计提激励,
       10 激励标准,
       C.END_DATE 结束时间
  FROM TMP_o2o_HHB_QQK_2 A, TMP_o2o_HHB_QQK_4 B, TMP_o2o_HHB_QQK_5 C
 WHERE A.DEVICE_NUMBER = B.SERIAL_NUMBER_B(+)
   AND A.DEVICE_NUMBER = C.SERIAL_NUMBER_B(+)            
         ';   
         
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create TMP_o2o_HHB_QQK_6 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         --加入封存表
          v_sql := 'delete from TMP_o2o_HHB_QQK_fengcun 
           where jifen_month = ''' || p_month || '''                       
            '; 
                      
           execute immediate v_sql ;
           commit;
         
         
         
         v_sql := 'insert into  TMP_o2o_HHB_QQK_fengcun 
         select ''' || p_month || ''' as jifen_month,a.*
         from TMP_o2o_HHB_QQK_6 a' ;
         
         execute immediate v_sql ;
         commit;
         
         
        exception 
        when others then     
           v_run_order := v_run_order + 1 ;         
           v_end_flag :=  0;
           v_remark := sqlerrm ;
           insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark);
           commit ;
           
           v_sql := 'delete from TMP_o2o_HHB_QQK_fengcun 
           where jifen_month = ''' || p_month || '''                       
            '; 
                      
           execute immediate v_sql ;
           commit;
         
           
    
    end p_o2o_qinqingka_yy;
    
    
    procedure p_o2o_wangka_yy(p_month in varchar2) is
      
         v_start_date date ; 
         v_pkg_name varchar2(50);
         v_prg_name varchar2(50);
         v_run_order number(5);
         v_remark varchar2(300);
         v_end_flag number(1);
         v_sql clob;
         
       begin
         
         v_start_date := sysdate ;
         v_pkg_name := 'pkg_chnl_o2o_data' ;
         v_prg_name := 'p_o2o_wangka_yy' ;
         v_run_order := 0 ;  
        
         IF p_month IS  NULL or length(p_month) <> 6 THEN
            raise_application_error(-20001,'p_month is error');
         END IF ;
         
         
         safely_drop('tmp_o2o_wangka_user');
         
         v_sql := 'create table tmp_o2o_wangka_user nologging as
         select * from (
         select a.*,b.user_id,b.serial_number,b.cust_id,b.remove_tag,
         b.user_state_name,b.deal_dt,b.product_name,b.bss_product_id,
         b.develop_depart_to_region_name
         from tmp_o2o_wangka_base a,report.t_user_all_'|| p_month ||' b
         where a.cbss_developer_id=b.cbss_developer_id
         and a.jifen_month = ''' || p_month || ''' 
         union all
         select a.*,b.user_id,b.serial_number,b.cust_id,b.remove_tag,
         b.user_state_name,b.deal_dt,b.product_name,b.bss_product_id,
         b.develop_depart_to_region_name
         from tmp_o2o_wangka_base a,report.t_user_all_'|| p_month ||' b
         where a.cbss_developer_name=b.bss_develop_depart_name
         and a.jifen_month = ''' || p_month || ''' and b.bss_product_id = ''70000135'' 
         ) where to_char(deal_dt,''yyyymm'') = ''' || p_month || '''    
         ';   
         
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_wangka_user OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         
         safely_drop('tmp_o2o_wangka_paylog');
         
         v_sql := 'create table tmp_o2o_wangka_paylog nologging as
 
         SELECT a.*,b.recv_fee,b.recv_time,b.recv_staff_id
          from tmp_o2o_wangka_user a,CBSSA_UCR_ACT3.TF_B_PAYLOG_ADD_BIL b
          WHERE a.user_id=b.user_id  
          AND b.pay_fee_mode_code<>4
          AND b.payment_op IN (16000,16001)
          and b.recv_fee>0
          AND B.RECV_TIME>=A.DEAL_DT
          AND B.RECV_TIME< A.DEAL_DT+4
          UNION ALL     
         SELECT a.*,b.recv_fee,b.recv_time,b.recv_staff_id
          from tmp_o2o_wangka_user a,act_ucr_act1.tf_b_paylog b
          WHERE a.user_id=b.user_id  
          AND b.pay_fee_mode_code<>4
          AND b.payment_op IN (16000,16001)
          and b.recv_fee>0
          AND B.RECV_TIME>=A.DEAL_DT
          AND B.RECV_TIME< A.DEAL_DT+4
                      
         ';   
         
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_wangka_paylog OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         safely_drop('tmp_o2o_wangka_paylog1');
         
         v_sql := 'create table tmp_o2o_wangka_paylog1 nologging as
         SELECT DISTINCT a.user_id,
          first_value(a.recv_fee) OVER(PARTITION BY user_id ORDER BY a.recv_time) fisrt_fee,
          first_value(a.recv_time) OVER(PARTITION BY user_id ORDER BY a.recv_time) fisrt_recv_time,
          SUM(a.recv_fee) OVER(PARTITION BY user_id) sum_fee,
          first_value(a.recv_fee) OVER(PARTITION BY user_id ORDER BY a.recv_time DESC) last_fee,
          first_value(a.recv_time) OVER(PARTITION BY user_id ORDER BY a.recv_time DESC) last_recv_time,
          SUM(CASE WHEN trunc(a.recv_time)-TRUNC(a.deal_dt)<=4 THEN a.recv_fee ELSE 0 END) OVER(PARTITION BY user_id) fee_in_5day,
          SUM(CASE WHEN TRUNC(a.recv_time,''MM'')=TRUNC(SYSDATE,''MM'') THEN a.recv_fee ELSE 0 END) OVER(PARTITION BY user_id) fee_month
          from tmp_o2o_wangka_paylog a            
         ';   
         
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_wangka_paylog1 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         safely_drop('tmp_o2o_wangka_group');
         
         v_sql := 'create table tmp_o2o_wangka_group nologging as
         select a.*, nvl(c.fisrt_fee/100,0) first_fee,
         nvl(b.status,0) sw_flag
         from tmp_o2o_wangka_user a , tmp_o2o_sw b,tmp_o2o_wangka_paylog1 c
         where a.user_id=b.user_id(+)
         and a.user_id=c.user_id(+)         
         ';   
         
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_wangka_group OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         
         ----放入封存表
          v_sql := 'delete from tmp_o2o_wangka_fengcun 
           where jifen_month = ''' || p_month || '''                       
            '; 
                      
         execute immediate v_sql ;
         commit;
         
         
         v_sql := 'insert into tmp_o2o_wangka_fengcun 
         select a.*,
(case when a.first_fee >= 50 and a.first_fee<100 and a.user_state_name like ''%正常%'' and a.sw_flag=0 
and a.bss_product_id in (''90063345'',''90350506'',''90155946'',''90361378'',''90361381'',''90372006'',''90372008'',''90337592'',''90337593'') then 30 

 when a.first_fee >= 100 and a.first_fee<200 and a.user_state_name like ''%正常%'' and a.sw_flag=0 
and a.bss_product_id in (''90063345'',''90350506'',''90155946'',''90361378'',''90361381'',''90372006'',''90372008'',''90337592'',''90337593'') then 50

 when a.first_fee >= 200 and a.user_state_name like ''%正常%'' and a.sw_flag=0 
and a.bss_product_id in (''90063345'',''90350506'',''90155946'',''90361378'',''90361381'',''90372006'',''90372008'',''90337592'',''90337593'') then 80

 when  a.user_state_name like ''%正常%'' and a.sw_flag=0 and a.bss_product_id in (''90393985'',''90393993'') then 100
      when a.user_state_name like ''%正常%'' and a.sw_flag=0 and a.bss_product_id in (''90333721'',''90359651'') then 50
        when a.user_state_name like ''%开通%'' and a.sw_flag=0 and a.bss_product_id in (''70000135'') then 100
else 0 end) jifen,
(case when a.first_fee>=50 and a.user_state_name like ''%正常%'' and a.sw_flag=0 then 1 else 0 end ) chk_flag
from tmp_o2o_wangka_group a    
     
         ';   
         
         execute immediate v_sql ;
         commit;
         
         
    
    exception 
        when others then     
           v_run_order := v_run_order + 1 ;         
           v_end_flag :=  0;
           v_remark := sqlerrm ;
           insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark);
           commit ;
           
           v_sql := 'delete from tmp_o2o_wangka_fengcun 
           where jifen_month = ''' || p_month || '''                       
            '; 
                      
           execute immediate v_sql ;
           commit;
    

    
    end p_o2o_wangka_yy;
    
    
    
    procedure p_o2o_zhongyou_sh(p_month in varchar2) is
      
    
         v_start_date date ; 
         v_pkg_name varchar2(50);
         v_prg_name varchar2(50);
         v_run_order number(5);
         v_remark varchar2(300);
         v_end_flag number(1);
         v_sql clob;
         
         
         begin
         
           v_start_date := sysdate ;
           v_pkg_name := 'pkg_chnl_o2o_data' ;
           v_prg_name := 'p_o2o_zhongyou_sh' ;
           v_run_order := 0 ;  
        
         IF p_month IS  NULL or length(p_month) <> 6 THEN
            raise_application_error(-20001,'p_month is error');
         END IF ;
         
         
         safely_drop('tmp_o2o_zhongyou_cuxiao_1');
         
         v_sql := 'create table tmp_o2o_zhongyou_cuxiao_1 nologging as
         select a.*,b.bianzhi from tmp_o2o_cuxiao_base a,tmp_o2o_zhongyou_cuxiao b
         where a.staff_id=b.staff_id and a.jifen_month = ''' || p_month || '''
         and b.jifen_month = '''|| p_month|| '''
               
         ';   
         
         execute immediate v_sql ;
         
        
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_zhongyou_cuxiao_1 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         
         safely_drop('tmp_o2o_zhongyou_base_a');
         
         v_sql := 'create table tmp_o2o_zhongyou_base_a nologging as
                    select a.* from 
                    tmp_o2o_cuxiao_fengcun a,tmp_o2o_zhongyou_cuxiao_1 b
                    where a.staff_id=b.staff_id and b.bianzhi=''众由'' and a.jifen_month = ''' || p_month || '''
               
         ';   

         
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_zhongyou_base_a OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         
         safely_drop('tmp_o2o_zhongyou_base1');
         
         v_sql := 'create table tmp_o2o_zhongyou_base1 nologging as
select distinct a.*,b.STREAM_TOTAL_FLUX data_count,b.VO_TOTAL_DURA call_count,
(case when b.STREAM_TOTAL_FLUX>10 or b.VO_TOTAL_DURA> 600 then 0 else 1 end ) sd_flag ,
(case when a.payfee>=50 then 1 else 0 end ) charge_tag
from tmp_o2o_zhongyou_base_a a,(select * from ucr_public.t4p_user_info_mon@Link_gdbi where partition_id=substr(' || p_month || ',5,6)
and timest='''|| p_month ||''') b
where  a.user_id=b.user_id(+)
         
              
         ';   
         
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_zhongyou_base1 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         
         safely_drop('tmp_o2o_zhongyou_base2');
         
         v_sql := 'create table tmp_o2o_zhongyou_base2 nologging as
       select ''51b8bwa'' as depart_id,count(1) as channeng,round((sum(sd_flag)/count(1)),2 ) sd_rate,
round((sum(charge_tag)/count(1)),2) charge_rate
from tmp_o2o_zhongyou_base1 a
               
         ';   

         
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_zhongyou_base2 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         safely_drop('tmp_o2o_zhongyou_base3');
         
         v_sql := 'create table tmp_o2o_zhongyou_base3 nologging as
       select a.*,b.channeng,b.sd_rate,b.charge_rate charge_rate_real,
round(b.channeng/a.cn_target,2) cn_rate 
from tmp_o2o_zhongyou_base a,tmp_o2o_zhongyou_base2 b
where a.depart_id=b.depart_id and a.jifen_month='''||  p_month ||'''
               
         ';   

         
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_zhongyou_base3 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         safely_drop('tmp_o2o_zhongyou_base4');
         
         v_sql := 'create table tmp_o2o_zhongyou_base4 nologging as
       select a.*,

(case when a.cn_rate>=0.9 then 1 
      when a.cn_rate>=0.8 and a.cn_rate<0.9 then 0.9
      when a.cn_rate>=0.7 and a.cn_rate<0.8 then 0.8
      when a.cn_rate>=0.6 and a.cn_rate<0.7 then 0.7
        else a.cn_rate  end)*30 as cn_score,
(case when a.charge_rate_real>=a.charge_rate then 15
when (a.charge_rate-charge_rate_real)*100>=15 then 0
  else 15-(a.charge_rate-charge_rate_real)*100

end) charge_score,

(case when a.sd_rate<=a.double_rate then 15
when (a.sd_rate-a.double_rate)*100>=15 then 0
  else 15-(a.sd_rate-a.double_rate)*100
end) sd_score

from tmp_o2o_zhongyou_base3 a
               
         ';   

         
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_zhongyou_base4 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         
         safely_drop('tmp_o2o_zhongyou_base5');
         
         v_sql := 'create table tmp_o2o_zhongyou_base5 nologging as
       select a.*,
(a.charge_score+a.sd_score+a.cn_score+a.punish+a.yw_rate+a.team_build) target_all,

(case when (a.charge_score+a.sd_score+a.cn_score+a.punish+a.yw_rate+a.team_build)
between 95 and 100 then 
1.05+(((a.charge_score+a.sd_score+a.cn_score+a.punish+a.yw_rate+a.team_build)-95)/(100-95))*(1.1-1.05)

when (a.charge_score+a.sd_score+a.cn_score+a.punish+a.yw_rate+a.team_build)>=90
  and (a.charge_score+a.sd_score+a.cn_score+a.punish+a.yw_rate+a.team_build)<95 then
1.0+(((a.charge_score+a.sd_score+a.cn_score+a.punish+a.yw_rate+a.team_build)-90)/(95-90))*(1.05-1.0)

when  (a.charge_score+a.sd_score+a.cn_score+a.punish+a.yw_rate+a.team_build)>=80
  and (a.charge_score+a.sd_score+a.cn_score+a.punish+a.yw_rate+a.team_build)<90 then
0.9+(((a.charge_score+a.sd_score+a.cn_score+a.punish+a.yw_rate+a.team_build)-80)/(90-80))*(1.0-0.9) 

when  (a.charge_score+a.sd_score+a.cn_score+a.punish+a.yw_rate+a.team_build)>=70
  and (a.charge_score+a.sd_score+a.cn_score+a.punish+a.yw_rate+a.team_build)<80 then
0.8+(((a.charge_score+a.sd_score+a.cn_score+a.punish+a.yw_rate+a.team_build)-70)/(80-70))*(0.9-0.8)

when  (a.charge_score+a.sd_score+a.cn_score+a.punish+a.yw_rate+a.team_build)>=60
  and (a.charge_score+a.sd_score+a.cn_score+a.punish+a.yw_rate+a.team_build)<70 then
0.7+(((a.charge_score+a.sd_score+a.cn_score+a.punish+a.yw_rate+a.team_build)-60)/(70-60))*(0.8-0.7)
 
else 0.5
  
end) target_index

from tmp_o2o_zhongyou_base4 a
               
         ';   

         
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_zhongyou_base5 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         safely_drop('tmp_o2o_zhongyou_base6');
         
         v_sql := 'create table tmp_o2o_zhongyou_base6 nologging as
       select ''51b8bwa'' depart_id,sum((case when a.product_id in 
(''103509'',''127661'',''116303'') and a.payfee>=50 then 1
else 0 end )*30 ) as wk_fee,
 sum((case when a.product_id in 
(''128184'') /*and a.payfee>=50*/ then 1
else 0 end )*10 ) as qq_fee,
  sum((case when a.product_id not in 
(''128184'') and a.payfee>=50 then 1
else 0 end )*30 ) +sum((case when a.product_id in 
(''128184'') /*and a.payfee>=50*/ then 1
else 0 end )*10 ) as jf_develop
from tmp_o2o_cuxiao_fengcun a
where jifen_month=''' || p_month || '''
               
         ';   

         
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_zhongyou_base6 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         safely_drop('tmp_o2o_zhongyou_base7');
         
         v_sql := 'create table tmp_o2o_zhongyou_base7 nologging as
       select a.jifen_month,a.depart_id,a.cuxiao_duration,a.night_duration,a.cn_target,a.charge_rate,
a.double_rate,a.yw_rate,round(a.team_build,2) team_build,a.punish,a.channeng,a.sd_rate,a.charge_rate_real,
a.cn_rate,a.cn_score,a.charge_score,a.sd_score,round(target_all,2) target_all,round(target_index,2) target_index,
round(((2550/110)*a.cuxiao_duration+(1000/88)*a.night_duration),2) as jf_base,
b.jf_develop,
(round(((2550/110)*a.cuxiao_duration+(1000/88)*a.night_duration),2)
+b.jf_develop)*1.01*a.target_index
as fee_all
from tmp_o2o_zhongyou_base5 a,tmp_o2o_zhongyou_base6 b
where a.depart_id=b.depart_id
               
         ';   

         
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_zhongyou_base7 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         --放入封存表
         
         v_sql := 'delete from tmp_o2o_zhongyou_fengcun 
           where jifen_month = ''' || p_month || '''                       
            '; 
                      
           execute immediate v_sql ;
           commit;
         
         v_sql :=' insert into tmp_o2o_zhongyou_fengcun 
         select * from tmp_o2o_zhongyou_base7
         
         ';
         
         execute immediate v_sql ;
         commit;
         
         
         
          
     exception 
        when others then     
           v_run_order := v_run_order + 1 ;         
           v_end_flag :=  0;
           v_remark := sqlerrm ;
           insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark);
           commit ;
           
           v_sql := 'delete from tmp_o2o_zhongyou_fengcun 
           where jifen_month = ''' || p_month || '''                       
            '; 
                      
           execute immediate v_sql ;
           commit;
    
    
    
    
    end p_o2o_zhongyou_sh;
    
    
    procedure p_o2o_honghai_sh(p_month in varchar2,kaohe_score in float DEFAULT 0,kaohe_rate in float DEFAULT 1) is
      
         v_start_date date ; 
         v_pkg_name varchar2(50);
         v_prg_name varchar2(50);
         v_run_order number(5);
         v_remark varchar2(300);
         v_end_flag number(1);
         v_sql clob;
         
         
         begin
         
           v_start_date := sysdate ;
           v_pkg_name := 'pkg_chnl_o2o_data' ;
           v_prg_name := 'p_o2o_honghai_sh' ;
           v_run_order := 0 ;  
           
         if   kaohe_score is null then 
            return ;
         end if ;
           
        
         IF p_month IS  NULL or length(p_month) <> 6 THEN
            raise_application_error(-20001,'p_month is error');
         END IF ;
         
         
         safely_drop('tmp_nchnl_staff_honghai');
         
         v_sql := 'create table tmp_nchnl_staff_honghai nologging as
         SELECT e.area_name,a.grid_code,a.bss_depart_id depart_id,b.userid,a.dept_name,
        nvl(a.uni_pky_code_g,a.uni_pky_code_3g) uni_pky_code_g,d.user_name,
               f.dept_type_name type_2g, g.dept_type_name type_3g,a.dept_code dept_code_xs,
               a.channel_kind,a.dept_type_3g
        FROM code_dept_snapshot@link_gzdb46 a, info_user_snapshot@link_gzdb46 b, 
        info_user_snapshot@link_gzdb46 d, code_area@link_gzdb46 e,
        code_dept_type@link_gzdb46 f, code_dept_type@link_gzdb46 g
        WHERE a.dept_code=b.dept_code
        and a.part_month = substr('''|| p_month ||''',5,2)
        and b.part_month = substr('''|| p_month ||''',5,2)
        and d.part_month = substr('''|| p_month ||''',5,2)
        AND nvl(a.uni_pky_code_g,a.uni_pky_code_3g)=d.userid
        AND a.area_code_g=e.area_code
        AND a.channel_kind=f.channel_kind AND a.dept_type_3g=g.channel_kind
        and a.status =1
               
         ';   
         
         execute immediate v_sql ;
         
        
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_nchnl_staff_honghai OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         safely_drop('tmp_honghai_0');
         
         v_sql := 'create table tmp_honghai_0 nologging as
         select '''|| p_month ||''' jifen_month,''经营性外包'' 合同类型,count(distinct userid) as 传统门店数量,count(distinct userid)*220*1.5 as 基础服务费
         from tmp_nchnl_staff_t1  a,INFO_USER_HONGHAI@link_gzdb46 b
         where a.userid=b.seller
         and b.type_name like ''%经营%''
         and type_2g not in(''营业代理商'',''固网代理商'',''促销'')
         and type_3g not in(''校园集团代理'',''家客代理'',''电子渠道-CPS'',''电子渠道代理商'')
               
         ';   
         
         execute immediate v_sql ;
         
        
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_honghai_0 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         
         
         
         
         
        safely_drop('tmp_honghai_1');
         
         v_sql := 'create table tmp_honghai_1 nologging as
         select distinct a.*,b.type_name,c.depart_id as bss_develop_id,c.dev_code as cbss_develop_id,d.chnl_code b2i_develop_id 
from (select * from tmp_nchnl_staff_honghai  )a,
(select * from INFO_USER_HONGHAI@link_gzdb46 where type_name like ''%经营%'') b, bss_depart_mapping@link_gzdb46 c,
crm_ucr_crm1.tf_chl_cu_channel@link_ngbss d
where a.userid=b.seller
and a.dept_code_xs = c.dept_code(+)
and c.depart_id=d.chnl_id(+)
               
         ';   
         
         execute immediate v_sql ;
         
        
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_honghai_1 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         safely_drop('tmp_honghai_2');
         
         v_sql := 'create table tmp_honghai_2 nologging as
select  distinct a.area_name,a.userid,a.type_name,a.dept_name,b.user_id,b.bss_product_id,b.price,b.cal_money,b.jf_mon,b.jf_type
from tmp_honghai_1  a,tmp_nchnl_dev_ydjf_all b
where a.uni_pky_code_g=b.uni_pky_code_g
and a.bss_develop_id = b.depart_id
/*and b.depart_type =0 */
union 
select  distinct a.area_name,a.userid,a.type_name,a.dept_name,b.user_id,b.bss_product_id,b.price,b.cal_money,b.jf_mon,b.jf_type
from tmp_honghai_1  a,tmp_nchnl_dev_ydjf_all b
where a.uni_pky_code_g=b.uni_pky_code_g
and a.b2i_develop_id = b.depart_id
and b.depart_type =5 
union 
select  distinct a.area_name,a.userid,a.type_name,a.dept_name,b.user_id,b.bss_product_id,b.price,b.cal_money,b.jf_mon,b.jf_type
from tmp_honghai_1  a,tmp_nchnl_dev_ydjf_all b
where a.uni_pky_code_g=b.uni_pky_code_g
and a.userid = b.depart_id
/*and b.depart_type =1 
*/union
select  distinct a.area_name,a.userid,a.type_name,a.dept_name,b.user_id,b.bss_product_id,b.price,b.cal_money,b.jf_mon,b.jf_type
from tmp_honghai_1  a,tmp_nchnl_dev_ydjf_all b
where a.uni_pky_code_g=b.uni_pky_code_g
and a.cbss_develop_id = b.depart_id
/*and b.depart_type = 20 
*/union
select  distinct a.area_name,a.userid,a.type_name,a.dept_name,b.user_id,b.bss_product_id,b.price,b.cal_money,b.jf_mon,b.jf_type
from tmp_honghai_1  a,tmp_nchnl_dev_ydjf_all b
where a.uni_pky_code_g=b.uni_pky_code_g
and a.cbss_develop_id = b.depart_id
/*and b.depart_type = 6 
*/
               
         ';   
         
         execute immediate v_sql ;
         
        
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_honghai_2 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
          safely_drop('tmp_honghai_3');
         
         v_sql := 'create table tmp_honghai_3 nologging as
         select a.type_name,a.userid,a.area_name,a.jf_mon,
sum((case when a.bss_product_id in (select product_id from tmp_o2o_honghai_product where product_type =''O2O产品'') and a.jf_type in (''王卡直销'') then 1 else 0 end)) zx_O2O_sum_num,
sum((case when a.bss_product_id in (select product_id from tmp_o2o_honghai_product where product_type =''O2O产品'') and a.jf_type in (''王卡直销'') then cal_money else 0 end)) zx_O2O_sum_money,
sum((case when a.bss_product_id in (select product_id from tmp_o2o_honghai_product where product_type =''O2O产品'') and a.jf_type in (''移动后付'',''移动预付'',''王卡分销'',''王卡超量'') then 1 else 0 end)) md_O2O_sum_num,
sum((case when a.bss_product_id in (select product_id from tmp_o2o_honghai_product where product_type =''O2O产品'') and a.jf_type in (''移动后付'',''移动预付'',''王卡分销'',''王卡超量'') then cal_money else 0 end)) md_O2O_sum_money,

sum((case when a.bss_product_id in (select product_id from tmp_o2o_honghai_product where product_type =''冰激凌产品'') and a.jf_type in (''王卡直销'') then 1 else 0 end)) zx_bjl_sum_num,
sum((case when a.bss_product_id in (select product_id from tmp_o2o_honghai_product where product_type =''冰激凌产品'') and a.jf_type in (''王卡直销'') then cal_money else 0 end)) zx_bjl_sum_money,
sum((case when a.bss_product_id in (select product_id from tmp_o2o_honghai_product where product_type =''冰激凌产品'') and a.jf_type in (''移动后付'',''移动预付'',''王卡分销'',''王卡超量'') then 1 else 0 end)) md_bjl_sum_num,
sum((case when a.bss_product_id in (select product_id from tmp_o2o_honghai_product where product_type =''冰激凌产品'') and a.jf_type in (''移动后付'',''移动预付'',''王卡分销'',''王卡超量'') then cal_money else 0 end)) md_bjl_sum_money,


sum((case when a.bss_product_id in (select product_id from tmp_o2o_honghai_product where product_type =''后付费49'') and a.jf_type in (''王卡直销'') then 1 else 0 end)) zx_hff_sum_num,
sum((case when a.bss_product_id in (select product_id from tmp_o2o_honghai_product where product_type =''后付费49'') and a.jf_type in (''王卡直销'') then cal_money else 0 end)) zx_hff_sum_money,
sum((case when a.bss_product_id in (select product_id from tmp_o2o_honghai_product where product_type =''后付费49'') and a.jf_type in (''移动后付'',''移动预付'',''王卡分销'',''王卡超量'') then 1 else 0 end)) md_hff_sum_num,
sum((case when a.bss_product_id in (select product_id from tmp_o2o_honghai_product where product_type =''后付费49'') and a.jf_type in (''移动后付'',''移动预付'',''王卡分销'',''王卡超量'') then cal_money else 0 end)) md_hff_sum_money,

sum((case when a.bss_product_id in (select product_id from tmp_o2o_honghai_product where product_type =''预付费'') and a.jf_type in (''王卡直销'') then 1 else 0 end)) zx_yff_sum_num,
sum((case when a.bss_product_id in (select product_id from tmp_o2o_honghai_product where product_type =''预付费'') and a.jf_type in (''王卡直销'') then cal_money else 0 end)) zx_yff_sum_money,
sum((case when a.bss_product_id in (select product_id from tmp_o2o_honghai_product where product_type =''预付费'') and a.jf_type in (''移动后付'',''移动预付'',''王卡分销'',''王卡超量'') then 1 else 0 end)) md_yff_sum_num,
sum((case when a.bss_product_id in (select product_id from tmp_o2o_honghai_product where product_type =''预付费'') and a.jf_type in (''移动后付'',''移动预付'',''王卡分销'',''王卡超量'') then cal_money else 0 end)) md_yff_sum_money,

sum((case when a.bss_product_id in (select product_id from tmp_o2o_honghai_product where product_type =''其他产品'') and a.jf_type in (''王卡直销'') then 1 else 0 end)) zx_qt_sum_num,
sum((case when a.bss_product_id in (select product_id from tmp_o2o_honghai_product where product_type =''其他产品'') and a.jf_type in (''王卡直销'') then cal_money else 0 end)) zx_qt_sum_money,
sum((case when a.bss_product_id in (select product_id from tmp_o2o_honghai_product where product_type =''其他产品'') and a.jf_type in (''移动后付'',''移动预付'',''王卡分销'',''王卡超量'') then 1 else 0 end)) md_qt_sum_num,
sum((case when a.bss_product_id in (select product_id from tmp_o2o_honghai_product where product_type =''其他产品'') and a.jf_type in (''移动后付'',''移动预付'',''王卡分销'',''王卡超量'') then cal_money else 0 end)) md_qt_sum_money,

sum((case when a.jf_type not in (''王卡直销'',''移动后付'',''移动预付'',''王卡分销'',''王卡超量'') then cal_money else 0 end)) jdx_sum_money


from tmp_honghai_2 a
group by a.type_name,a.userid,a.area_name,a.jf_mon
               
         ';   
         
         execute immediate v_sql ;
         
        
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_honghai_3 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         safely_drop('tmp_honghai_4');
         
         v_sql := 'create table tmp_honghai_4 nologging as
select sum(a.zx_O2O_sum_num) zx_O2O_sum_num, 
round((case when sum(a.zx_O2O_sum_num) = 0 then 0 else sum(a.zx_O2O_sum_money)/sum(a.zx_O2O_sum_num) end),6) zw_O2O_price,

sum(a.zx_bjl_sum_num) zx_bjl_sum_num, 
round((case when sum(a.zx_bjl_sum_num) = 0 then 0 else sum(a.zx_bjl_sum_money)/sum(a.zx_bjl_sum_num) end),6) zw_bjl_price,

sum(a.zx_hff_sum_num) zx_hff_sum_num, 
round((case when sum(a.zx_hff_sum_num) = 0 then 0 else sum(a.zx_hff_sum_money)/sum(a.zx_hff_sum_num) end),6) zw_hff_price,

sum(a.zx_yff_sum_num) zx_yff_sum_num, 
round((case when sum(a.zx_yff_sum_num) = 0 then 0 else sum(a.zx_yff_sum_money)/sum(a.zx_yff_sum_num) end),6) zw_yff_price,

sum(a.zx_qt_sum_num) zx_qt_sum_num, 
round((case when sum(a.zx_qt_sum_num) = 0 then 0 else sum(a.zx_qt_sum_money)/sum(a.zx_qt_sum_num) end),6) zw_qt_price,


sum(a.md_O2O_sum_num) md_O2O_sum_num, 
round((case when sum(a.md_O2O_sum_num) = 0 then 0 else sum(a.md_O2O_sum_money)/sum(a.md_O2O_sum_num) end),6) md_O2O_price,

sum(a.md_bjl_sum_num) md_bjl_sum_num, 
round((case when sum(a.md_bjl_sum_num) = 0 then 0 else sum(a.md_bjl_sum_money)/sum(a.md_bjl_sum_num) end),6) md_bjl_price,

sum(a.md_hff_sum_num) md_hff_sum_num, 
round((case when sum(a.md_hff_sum_num) = 0 then 0 else sum(a.md_hff_sum_money)/sum(a.md_hff_sum_num) end),6) md_hff_price,

sum(a.md_yff_sum_num) md_yff_sum_num, 
round((case when sum(a.md_yff_sum_num) = 0 then 0 else sum(a.md_yff_sum_money)/sum(a.md_yff_sum_num) end),6) md_yff_price,

sum(a.md_qt_sum_num) md_qt_sum_num, 
round((case when sum(a.md_qt_sum_num) = 0 then 0 else sum(a.md_qt_sum_money)/sum(a.md_qt_sum_num) end),6) md_qt_price,

(sum(a.zx_O2O_sum_money)+sum(a.md_O2O_sum_money)+sum(a.zx_bjl_sum_money)+sum(a.md_bjl_sum_money)+
sum(a.zx_hff_sum_money)+sum(a.md_hff_sum_money)+sum(a.zx_yff_sum_money)+sum(a.md_yff_sum_money)+
sum(a.zx_qt_sum_money)+sum(a.md_qt_sum_money)) *1 ywzc_all,
sum(a.jdx_sum_money)*1.5 as jdx_money,sum(a.jdx_sum_money)*1.5*1 as jdx_all

from tmp_honghai_3 a
         ';   
         
         execute immediate v_sql ;
         
        
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_honghai_4 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         
        safely_drop('tmp_honghai_4tmp');
         
         v_sql := 'create table tmp_honghai_4tmp nologging as
        select '''|| p_month ||''' jifen_month,''经营性外包'' 合同类型,
a.zx_o2o_sum_num 直销发展_O2O产品_数量,a.zw_o2o_price 直销发展_O2O产品_单价,a.md_o2o_sum_num 门店发展_O2O产品_数量,a.md_o2o_price 门店发展_O2O产品_单价,
a.zx_bjl_sum_num 直销发展_冰激凌产品_数量,a.zw_bjl_price 直销发展_冰激凌产品_单价,a.md_bjl_sum_num 门店发展_冰激凌产品_数量,a.md_bjl_price 门店发展_冰激凌产品_单价,
a.zx_hff_sum_num 直销发展_后付费产品_数量,a.zw_hff_price 直销发展_后付费产品_单价,a.md_hff_sum_num 门店发展_后付费产品_数量,a.md_hff_price 门店发展_后付费产品_单价,
a.zx_yff_sum_num 直销发展_预付费产品_数量,a.zw_yff_price 直销发展_预付费产品_单价,a.md_yff_sum_num 门店发展_预付费产品_数量,a.md_yff_price 门店发展_预付费产品_单价,
a.zx_qt_sum_num 直销发展_其他产品_数量,a.zw_qt_price 直销发展_其他产品_单价,a.md_qt_sum_num 门店发展_其他产品_数量,a.md_qt_price 门店发展_其他产品_单价,
a.ywzc_all 业务支撑费用总计,a.jdx_money 业绩奖励,a.jdx_all 阶段性奖励

from tmp_honghai_4 a
               
         ';   
         
         execute immediate v_sql ;
         
        
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_honghai_4tmp OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         safely_drop('tmp_honghai_5');
         
         v_sql := 'create table tmp_honghai_5 nologging as
        select  distinct a.area_name,a.userid,a.type_name,a.dept_name,b.user_id_c,b.serial_number,b.product_id,b.cal_money,b.jf_mon,b.jf_type
from tmp_honghai_1  a,tmp_nchnl_yd_others_all b
where a.uni_pky_code_g=b.uni_pky_code_g
and a.bss_develop_id = b.depart_id
/*and b.depart_type =0 */
union 
select  distinct a.area_name,a.userid,a.type_name,a.dept_name,b.user_id_c,b.serial_number,b.product_id,b.cal_money,b.jf_mon,b.jf_type
from tmp_honghai_1  a,tmp_nchnl_yd_others_all b
where a.uni_pky_code_g=b.uni_pky_code_g
and a.b2i_develop_id = b.depart_id
and b.depart_type =5 
union 
select  distinct a.area_name,a.userid,a.type_name,a.dept_name,b.user_id_c,b.serial_number,b.product_id,b.cal_money,b.jf_mon,b.jf_type
from tmp_honghai_1  a,tmp_nchnl_yd_others_all b
where a.uni_pky_code_g=b.uni_pky_code_g
and a.userid = b.depart_id
/*and b.depart_type =1 
*/union
select  distinct a.area_name,a.userid,a.type_name,a.dept_name,b.user_id_c,b.serial_number,b.product_id,b.cal_money,b.jf_mon,b.jf_type
from tmp_honghai_1  a,tmp_nchnl_yd_others_all b
where a.uni_pky_code_g=b.uni_pky_code_g
and a.cbss_develop_id = b.depart_id
/*and b.depart_type = 20 
*/union
select  distinct a.area_name,a.userid,a.type_name,a.dept_name,b.user_id_c,b.serial_number,b.product_id,b.cal_money,b.jf_mon,b.jf_type
from tmp_honghai_1  a,tmp_nchnl_yd_others_all b
where a.uni_pky_code_g=b.uni_pky_code_g
and a.cbss_develop_id = b.depart_id
               
         ';   
         
         execute immediate v_sql ;
         
        
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_honghai_5 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         safely_drop('tmp_honghai_6');
         
         v_sql := 'create table tmp_honghai_6 nologging as
select '''|| p_month ||''' jifen_month,''经营性外包'' 合同类型,sum(cal_money)/0.005 as 在网收入 ,sum(cal_money)*1*1.5 as 在网收入总计,
'''|| kaohe_score || ''' 考核得分,'''|| kaohe_rate || ''' 考核系数
from tmp_honghai_5 
               
         ';   
         
      
        execute immediate v_sql ;
         
         
         
        
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_honghai_6 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
          safely_drop('tmp_honghai_all');
         
         v_sql := 'create table tmp_honghai_all nologging as
select ''51b0e03'' depart_id,b.*,a.传统门店数量,a.基础服务费,c.在网收入,c.在网收入总计,
round(c.考核得分,2) 考核得分 ,round(c.考核系数,2) 考核系数,
round((b.业务支撑费用总计+ b.业绩奖励 + a.基础服务费+c.在网收入总计)*c.考核系数,2) as 结算费用
from tmp_honghai_0 a,tmp_honghai_4tmp b,tmp_honghai_6 c
where a.jifen_month=b.jifen_month and a.jifen_month=c.jifen_month 
and a.合同类型=b.合同类型 and a.合同类型=c.合同类型
and a.jifen_month='''|| p_month ||''' and a.合同类型=''经营性外包''
               
         ';   
         
         execute immediate v_sql ;
         
          --封存

         
         v_sql := 'delete from tmp_honghai_all_fengcun 
           where jifen_month = ''' || p_month || '''                       
            '; 
                      
           execute immediate v_sql ;
           commit;
         
         v_sql :=' insert into tmp_honghai_all_fengcun 
         select * from tmp_honghai_all
         
         ';
         
         execute immediate v_sql ;
         commit; 
         
        
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_honghai_all OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
        
         
    
    
    
    
    exception 
        when others then     
           v_run_order := v_run_order + 1 ;         
           v_end_flag :=  0;
           v_remark := sqlerrm ;
           insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark);
           commit ;
           
           v_sql := 'delete from tmp_honghai_all_fengcun
           where jifen_month = ''' || p_month || '''                       
            '; 
                      
           execute immediate v_sql ;
           commit;
    
    
    
    end p_o2o_honghai_sh;
    
    
    
    
    
    
    
    
    
    
    procedure p_o2o_lianmeng_sh(p_month in varchar2) is
      
         v_start_date date ; 
         v_pkg_name varchar2(50);
         v_prg_name varchar2(50);
         v_run_order number(5);
         v_remark varchar2(300);
         v_end_flag number(1);
         v_sql clob;
         
         
         begin
         
           v_start_date := sysdate ;
           v_pkg_name := 'pkg_chnl_o2o_data' ;
           v_prg_name := ' p_o2o_lianmeng_sh' ;
           v_run_order := 0 ;  
        
         IF p_month IS  NULL or length(p_month) <> 6 THEN
            raise_application_error(-20001,'p_month is error');
         END IF ;
         
         
         ---当月发展用户
         
         safely_drop('tmp_o2o_lianmeng_user');
         
         v_sql := ' create table tmp_o2o_lianmeng_user nologging as
         select * from (
          select a.*,b.user_id,b.serial_number,b.cust_id,b.remove_tag,
         b.user_state_name,b.deal_dt,b.product_id,b.product_name
         from tmp_o2o_lianmeng_base a,report.t_user_all_'|| p_month ||' b
         where a.cbss_develop_depart_id=b.cbss_develop_depart_id
         and a.jifen_month = ''' || p_month || '''
         ) where to_char(deal_dt,''yyyymm'') = ''' || p_month || '''
                  
         ';
         
         
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_lianmeng_user  OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ; 
         
         
         
         
         
         safely_drop('tmp_o2o_lianmeng_paylog');
         
         v_sql := ' create table tmp_o2o_lianmeng_paylog nologging as
          SELECT a.*,b.recv_fee,b.recv_time,b.recv_staff_id
          from tmp_o2o_lianmeng_user a,CBSSA_UCR_ACT3.TF_B_PAYLOG_ADD_BIL b
          WHERE a.user_id=b.user_id
          AND b.pay_fee_mode_code<>4
          AND b.payment_op IN (16000,16001)
          and b.recv_fee>0
          AND B.RECV_TIME>=A.DEAL_DT
          AND B.RECV_TIME< A.DEAL_DT+4 
                  
         ';
         
         
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_lianmeng_paylog OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         --
         safely_drop('tmp_o2o_lianmeng_paylog_month');
         
         v_sql := ' create table tmp_o2o_lianmeng_paylog_month nologging as
         select b.user_id,sum(b.recv_fee)/100 payfee_month
         from (select distinct * from tmp_o2o_lianmeng_user) a,
         tmp_o2o_paylog b
         where a.user_id = b.user_id
         and b.pay_mon in ( '''|| p_month ||''')
         group by b.user_id 
                  
         ';
                 
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_lianmeng_paylog_month OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         --
         safely_drop('tmp_o2o_lianmeng_paylog1');
         
         v_sql := ' create table tmp_o2o_lianmeng_paylog1 nologging as
         SELECT DISTINCT a.user_id,
          first_value(a.recv_fee) OVER(PARTITION BY user_id ORDER BY a.recv_time) fisrt_fee,
          first_value(a.recv_time) OVER(PARTITION BY user_id ORDER BY a.recv_time) fisrt_recv_time,
          SUM(a.recv_fee) OVER(PARTITION BY user_id) sum_fee,
          first_value(a.recv_fee) OVER(PARTITION BY user_id ORDER BY a.recv_time DESC) last_fee,
          first_value(a.recv_time) OVER(PARTITION BY user_id ORDER BY a.recv_time DESC) last_recv_time,
          SUM(CASE WHEN trunc(a.recv_time)-TRUNC(a.deal_dt)<=4 THEN a.recv_fee ELSE 0 END) OVER(PARTITION BY user_id) fee_in_5day,
          SUM(CASE WHEN TRUNC(a.recv_time,''MM'')=TRUNC(SYSDATE,''MM'') THEN a.recv_fee ELSE 0 END) OVER(PARTITION BY user_id) fee_month
          from tmp_o2o_lianmeng_paylog a 
                  
         ';
                 
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_lianmeng_paylog1 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         --
         safely_drop('tmp_o2o_lianmeng_sw');
         
         v_sql := ' create table tmp_o2o_lianmeng_sw nologging as
         select a.user_id,nvl(b.status,0) status
         from (select distinct * from tmp_o2o_lianmeng_user) a,tmp_o2o_sw b
         where a.user_id=b.user_id(+)  
                  
         ';
                 
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_lianmeng_paylog1 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         ---

         safely_drop('tmp_o2o_lianmeng_group');
         
         v_sql := ' create table tmp_o2o_lianmeng_group nologging as
          select a.*, nvl(c.fisrt_fee/100,0) first_fee,
  nvl(b.status,0) sw_flag,nvl(d.payfee_month,0) payfee_month
from tmp_o2o_lianmeng_user a , tmp_o2o_lianmeng_sw b,tmp_o2o_lianmeng_paylog1 c,tmp_o2o_lianmeng_paylog_month d
where a.user_id=b.user_id(+)
and a.user_id=c.user_id(+)
and a.user_id=d.user_id(+)
                  
         ';
                 
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_lianmeng_group OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         ---放入封存表中
         v_sql := 'delete from tmp_o2o_lianmeng_mx_fengcun 
           where jifen_month = ''' || p_month || '''                       
            '; 
                      
         execute immediate v_sql ;
         commit;
         
         
         
         v_sql := 'insert into tmp_o2o_lianmeng_mx_fengcun 
         select *
         from tmp_o2o_lianmeng_group a' ;
         
         execute immediate v_sql ;
         commit;
         
         ---

         
         safely_drop('tmp_o2o_lianmeng_group1');
         
         v_sql := ' create table tmp_o2o_lianmeng_group1 nologging as
         select a.cbss_develop_depart_name,
          count(1) as develop_num  
          from tmp_o2o_lianmeng_group a
          where a.first_fee>=50
          group by a.cbss_develop_depart_name
                  
         ';
                 
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_lianmeng_group1 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         ---
         
         

         safely_drop('tmp_o2o_lianmeng_group2');
         
         v_sql := ' create table tmp_o2o_lianmeng_group2 nologging as
         select a.*,
(case when a.develop_num <500 then 0
 when a.develop_num>=1000 then 30 else 18 end) jifen_rate  
from tmp_o2o_lianmeng_group1 a
                  
         ';
                 
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_lianmeng_group2 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;

       ---合同数直接按照30的规则
       safely_drop('tmp_o2o_lianmeng_group2_ht');
         
         v_sql := ' create table tmp_o2o_lianmeng_group2_ht nologging as
         select a.*,30 jifen_rate  
         from tmp_o2o_lianmeng_group1 a
                  
         ';
                 
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_lianmeng_group2_ht OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
       
       
       
             
       ---
       
       
        safely_drop('tmp_o2o_lianmeng_group3');
         
         v_sql := ' create table tmp_o2o_lianmeng_group3 nologging as
               select  a.cbss_develop_depart_name,count(1) as develop_num1
        from tmp_o2o_lianmeng_group a
        where a.first_fee>=50 and a.sw_flag=0 and a.user_state_name like ''%正常%''
        group by a.cbss_develop_depart_name
                  
         ';
                 
         execute immediate v_sql ;
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_lianmeng_group3 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         --
         
         safely_drop('tmp_o2o_lianmeng_group4');
         
         v_sql := ' create table tmp_o2o_lianmeng_group4 nologging as
         select distinct a.cbss_develop_depart_name,b.develop_num ,b.jifen_rate,c.develop_num1,
b.jifen_rate*c.develop_num1 as jifen 
from tmp_o2o_lianmeng_base a,tmp_o2o_lianmeng_group2 b,tmp_o2o_lianmeng_group3 c
where a.cbss_develop_depart_name=b.cbss_develop_depart_name
and a.cbss_develop_depart_name = c.cbss_develop_depart_name
                  
         ';
                 
         execute immediate v_sql ;
         
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_lianmeng_group4 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         
         ---
         safely_drop('tmp_o2o_lianmeng_group4_ht');
         
         v_sql := ' create table tmp_o2o_lianmeng_group4_ht nologging as
         select distinct a.cbss_develop_depart_name,b.develop_num ,b.jifen_rate,c.develop_num1,
b.jifen_rate*c.develop_num1 as jifen 
from tmp_o2o_lianmeng_base a,tmp_o2o_lianmeng_group2_ht b,tmp_o2o_lianmeng_group3 c
where a.cbss_develop_depart_name=b.cbss_develop_depart_name
and a.cbss_develop_depart_name = c.cbss_develop_depart_name
                  
         ';
                 
         execute immediate v_sql ;
         
         
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_lianmeng_group4_ht OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         --加入封存表

          v_sql := 'delete from tmp_o2o_lianmeng_fengcun 
           where jifen_month = ''' || p_month || '''                       
            '; 
                      
          execute immediate v_sql ;
          commit;
         
         
         
         v_sql := 'insert into tmp_o2o_lianmeng_fengcun 
         select ''' || p_month || ''' as jifen_month,a.*
         from tmp_o2o_lianmeng_group4 a' ;
         
         execute immediate v_sql ;
         commit;
         
         
          --加入封存表合同

          v_sql := 'delete from tmp_o2o_lianmeng_fengcun_ht 
           where jifen_month = ''' || p_month || '''                       
            '; 
                      
          execute immediate v_sql ;
          commit;
         
         
         
         v_sql := 'insert into tmp_o2o_lianmeng_fengcun_ht 
         select ''' || p_month || ''' as jifen_month,a.*
         from tmp_o2o_lianmeng_group4_ht a' ;
         
         execute immediate v_sql ;
         commit;
    

         
         
    
     exception 
        when others then
               
           v_run_order := v_run_order + 1 ;         
           v_end_flag :=  0;
           v_remark := sqlerrm ;
           insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark);
           commit ;
           
           
           v_sql := 'delete from tmp_o2o_lianmeng_fengcun 
           where jifen_month = ''' || p_month || '''                       
            '; 
                      
           execute immediate v_sql ;
           commit;
           
           v_sql := 'delete from tmp_o2o_lianmeng_fengcun_ht 
           where jifen_month = ''' || p_month || '''                       
            '; 
                      
           execute immediate v_sql ;
           commit;
           
           
           v_sql := 'delete from tmp_o2o_lianmeng_mx_fengcun 
           where jifen_month = ''' || p_month || '''                       
            '; 
                      
           execute immediate v_sql ;
           commit;
           
           
    
    
    
    end p_o2o_lianmeng_sh;
    
    
    procedure p_o2o_peisong_yy(p_month in varchar2) is
      
         v_start_date date ; 
         v_next_month varchar2(40);
         v_pkg_name varchar2(50);
         v_prg_name varchar2(50);
         v_run_order number(5);
         v_remark varchar2(300);
         v_end_flag number(1);
         v_sql clob;
    
       begin
         
           v_start_date := sysdate ;
           v_pkg_name := 'pkg_chnl_o2o_data' ;
           v_prg_name := 'p_o2o_peisong_yy' ;
           v_run_order := 0 ;
           v_next_month := to_char(add_months(to_date(p_month,'yyyymm'),1),'yyyymm');
         
       
         
         IF p_month IS  NULL or length(p_month) <> 6 THEN
            raise_application_error(-20001,'p_month is error');
         END IF ;
         
         
         ---营销率/考核率
         safely_drop('tmp_o2o_peisong_yx_1');
         
         v_sql := 'create table tmp_o2o_peisong_yx_1 nologging as
        select jifen_month,发展人编码,人员类型 ,count(1) as 发展王卡总数
from  (select a.*,b.user_id,b.bss_product_name from TMP_o2o_HHB_XSTC_fengcun a,report.t_user_all_' || p_month || ' b
where a.订购号码=b.serial_number and b.bss_product_id in (''90063345'',''90155946'',''90350506'',''90337592'',''90337593'')
and B.REMOVE_TAG=''0'' and b.busitype=''4'' ) a where jifen_month=''' || p_month || '''
and a.类型=''上门配送'' /* and (a.bss_product_name like ''%王卡%'' or a.bss_product_name like ''%阿里%'')*/
group by jifen_month,发展人编码,人员类型  
               
         ';  
         
         
         execute immediate v_sql ;
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_peisong_yx_1 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         safely_drop('tmp_o2o_peisong_yx_2');
         
         v_sql := 'create table tmp_o2o_peisong_yx_2 nologging as
       select x.*,y.user_id_b,y.serial_number_b 
from (
select a.*,b.user_id from TMP_o2o_HHB_XSTC_fengcun a,report.t_user_all_' || p_month || ' b
where a.订购号码=b.serial_number
and B.REMOVE_TAG=''0'' and b.busitype=''4'' and a.jifen_month= '''|| p_month || ''') x,cbssc_ucr_crm3.tf_f_relation_uu@link_4gbss_all y
where x.user_id = y.user_id_a
and to_char(start_date,''yyyymm'') = '''|| v_next_month || ''' 
and x.user_id<>y.user_id_b
and x.类型=''上门配送''
               
         ';  
         
         
         execute immediate v_sql ;
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_peisong_yx_2 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         safely_drop('tmp_o2o_peisong_yx_3');
         
         v_sql := 'create table tmp_o2o_peisong_yx_3 nologging as
      select 发展人编码,人员类型,count(1) 副卡产能 from (
select a.*,b.bss_product_name product_name_b,b.user_state_name user_state_b
from tmp_o2o_peisong_yx_2 a,report.t_user_all_'|| p_month ||' b
where a.user_id_b=b.user_id
and b.bss_product_name like ''%亲情卡%'')
group by 发展人编码,人员类型
               
         ';  
         
         
         execute immediate v_sql ;
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_peisong_yx_3 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         
         safely_drop('tmp_o2o_peisong_yx_4');
         
         v_sql := 'create table tmp_o2o_peisong_yx_4 nologging as
select x.*,
(case when 发展王卡总数=0 then 0 else round(副卡产能/发展王卡总数,4) end) 二次营销率
from (
select a.*,nvl(b.副卡产能,0) 副卡产能 
from tmp_o2o_peisong_yx_1 a,tmp_o2o_peisong_yx_3 b
where a.发展人编码=b.发展人编码(+)) x
               
         ';  
         
         
         execute immediate v_sql ;
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_peisong_yx_4 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         ---转化率
         
         safely_drop('tmp_o2o_hhb_zhuanhua_base1');
         
         v_sql := 'create table tmp_o2o_hhb_zhuanhua_base1 nologging as
select * from tmp_o2o_hhb_zhuanhua_base
where jifen_month= '''||p_month||''' 
and product_name not like ''%亲情%''
               
         ';  
         
         
         execute immediate v_sql ;
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_hhb_zhuanhua_base1 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         safely_drop('tmp_o2o_hhb_zhuanhua_base2');
         
         v_sql := 'create table tmp_o2o_hhb_zhuanhua_base2 nologging as
select a.*,b.user_id,b.deal_dt 
from tmp_o2o_hhb_zhuanhua_base1 a,report.t_user_all@link_gzdbdc197 b
where a.user_state_name =''已激活'' 
and a.in_date<=to_char(add_months(to_date('||p_month||',''yyyymm''),1),''yyyymm'')||''05''
and a.serial_number=b.serial_number
and to_char(b.deal_dt,''yyyymm'') >=  '''|| p_month ||'''
               
         ';  
         
         
        
         execute immediate v_sql ;
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_hhb_zhuanhua_base2 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         
         /*201902修改 为T+1日内首次充值50元及以上*/
         
         if p_month <>  '201902' then 
         
             safely_drop('tmp_o2o_hhb_zhuanhua_base3');
             
             v_sql := 'create table tmp_o2o_hhb_zhuanhua_base3 nologging as
              SELECT a.*,b.recv_fee,b.recv_time,b.recv_staff_id
              from tmp_o2o_hhb_zhuanhua_base2 a,CBSSA_UCR_ACT3.TF_B_PAYLOG_ADD_BIL b
              WHERE a.user_id=b.user_id
              AND b.pay_fee_mode_code<>4
              AND b.payment_op IN (16000,16001)
              and b.recv_fee>0
              and b.recv_time>=a.deal_dt
              and b.recv_time< a.deal_dt+2
              /*AND to_char(B.RECV_TIME,''yyyymm'')>=a.jifen_month
              AND to_char(B.RECV_TIME,''yyyymm'')<= to_char(add_months(to_date('||p_month||',''yyyymm''),1),''yyyymm'')||''05''*/
                   
             ';  
             
             
             execute immediate v_sql ;
             
             v_run_order := v_run_order + 1 ;
             v_end_flag := 1 ;
             v_remark := 'create tmp_o2o_hhb_zhuanhua_base3 OK  ';
             insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
             commit ;
             
             
             safely_drop('tmp_o2o_hhb_zhuanhua_base4');
             
             v_sql := 'create table tmp_o2o_hhb_zhuanhua_base4 nologging as
             select x.*,y.first_fee,y.first_recv_time from  tmp_o2o_hhb_zhuanhua_base2 x ,(
              SELECT DISTINCT a.user_id,
              first_value(a.recv_fee) OVER(PARTITION BY user_id ORDER BY a.recv_time) first_fee,
              first_value(a.recv_time) OVER(PARTITION BY user_id ORDER BY a.recv_time) first_recv_time,
              SUM(a.recv_fee) OVER(PARTITION BY user_id) sum_fee,
              first_value(a.recv_fee) OVER(PARTITION BY user_id ORDER BY a.recv_time DESC) last_fee,
              first_value(a.recv_time) OVER(PARTITION BY user_id ORDER BY a.recv_time DESC) last_recv_time,
              SUM(CASE WHEN TRUNC(a.recv_time,''MM'')=TRUNC(SYSDATE,''MM'') THEN a.recv_fee ELSE 0 END) OVER(PARTITION BY user_id) fee_month
              from tmp_o2o_hhb_zhuanhua_base3 a
              ) y   where x.user_id=y.user_id 
              and y.first_fee/100>=50
                   
             ';  
             
             
             execute immediate v_sql ;
             
             v_run_order := v_run_order + 1 ;
             v_end_flag := 1 ;
             v_remark := 'create tmp_o2o_hhb_zhuanhua_base4 OK  ';
             insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
             commit ;
             
             
          else 
            
             
            safely_drop('tmp_o2o_hhb_zhuanhua_base3');
             
             v_sql := 'create table tmp_o2o_hhb_zhuanhua_base3 nologging as
              SELECT a.*,b.recv_fee,b.recv_time,b.recv_staff_id
              from tmp_o2o_hhb_zhuanhua_base2 a,CBSSA_UCR_ACT3.TF_B_PAYLOG_ADD_BIL b
              WHERE a.user_id=b.user_id
              AND b.pay_fee_mode_code<>4
              AND b.payment_op IN (16000,16001)
              and b.recv_fee>0
              and b.recv_time>=a.deal_dt
              and b.recv_time< a.deal_dt+2
              and to_char(a.deal_dt,''yyyymmdd'')>=''20190213'' 
              union all
              SELECT a.*,b.recv_fee,b.recv_time,b.recv_staff_id
              from tmp_o2o_hhb_zhuanhua_base2 a,CBSSA_UCR_ACT3.TF_B_PAYLOG_ADD_BIL b
              WHERE a.user_id=b.user_id
              AND b.pay_fee_mode_code<>4
              AND b.payment_op IN (16000,16001)
              and b.recv_fee>0
              and b.recv_time>=a.deal_dt
              and b.recv_time< a.deal_dt+6
              and to_char(a.deal_dt,''yyyymmdd'')<''20190213''
              /*AND to_char(B.RECV_TIME,''yyyymm'')>=a.jifen_month
              AND to_char(B.RECV_TIME,''yyyymm'')<= to_char(add_months(to_date('||p_month||',''yyyymm''),1),''yyyymm'')||''05''*/
                   
             ';  
             
             
             execute immediate v_sql ;
             
             v_run_order := v_run_order + 1 ;
             v_end_flag := 1 ;
             v_remark := 'create tmp_o2o_hhb_zhuanhua_base3 OK  ';
             insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
             commit ;
             
             
             safely_drop('tmp_o2o_hhb_zhuanhua_base4');
             
             v_sql := 'create table tmp_o2o_hhb_zhuanhua_base4 nologging as
             select x.*,y.first_fee,y.first_recv_time from  tmp_o2o_hhb_zhuanhua_base2 x ,(
              SELECT DISTINCT a.user_id,
              first_value(a.recv_fee) OVER(PARTITION BY user_id ORDER BY a.recv_time) first_fee,
              first_value(a.recv_time) OVER(PARTITION BY user_id ORDER BY a.recv_time) first_recv_time,
              SUM(a.recv_fee) OVER(PARTITION BY user_id) sum_fee,
              first_value(a.recv_fee) OVER(PARTITION BY user_id ORDER BY a.recv_time DESC) last_fee,
              first_value(a.recv_time) OVER(PARTITION BY user_id ORDER BY a.recv_time DESC) last_recv_time,
              SUM(CASE WHEN TRUNC(a.recv_time,''MM'')=TRUNC(SYSDATE,''MM'') THEN a.recv_fee ELSE 0 END) OVER(PARTITION BY user_id) fee_month
              from tmp_o2o_hhb_zhuanhua_base3 a
              where  to_char(a.deal_dt,''yyyymmdd'') >= ''20190213''
              union
              SELECT DISTINCT a.user_id,
              SUM(CASE WHEN trunc(a.recv_time)-TRUNC(a.deal_dt)<=6 THEN a.recv_fee ELSE 0 END) OVER(PARTITION BY user_id) fisrt_fee,
              first_value(a.recv_time) OVER(PARTITION BY user_id ORDER BY a.recv_time) first_recv_time,
              SUM(a.recv_fee) OVER(PARTITION BY user_id) sum_fee,
              first_value(a.recv_fee) OVER(PARTITION BY user_id ORDER BY a.recv_time DESC) last_fee,
              first_value(a.recv_time) OVER(PARTITION BY user_id ORDER BY a.recv_time DESC) last_recv_time,
              SUM(CASE WHEN TRUNC(a.recv_time,''MM'')=TRUNC(SYSDATE,''MM'') THEN a.recv_fee ELSE 0 END) OVER(PARTITION BY user_id) fee_month
              from tmp_o2o_hhb_zhuanhua_base3 a
              where  to_char(a.deal_dt,''yyyymmdd'') < ''20190213''
              
              ) y   where x.user_id=y.user_id 
              and y.first_fee/100>=50
                   
             ';  
             
             
             execute immediate v_sql ;
             
             v_run_order := v_run_order + 1 ;
             v_end_flag := 1 ;
             v_remark := 'create tmp_o2o_hhb_zhuanhua_base4 OK  ';
             insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
             commit ;
              
             
             
         
         end if;
         
         
         
         
         safely_drop('tmp_o2o_hhb_zhuanhua_base5');
         
         v_sql := 'create table tmp_o2o_hhb_zhuanhua_base5 nologging as
select jifen_month,cbss_developer_id,count(1) fenzi 
from tmp_o2o_hhb_zhuanhua_base4
group by jifen_month,cbss_developer_id
               
         ';  
         
         
         execute immediate v_sql ;
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_hhb_zhuanhua_base5 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         safely_drop('tmp_o2o_hhb_zhuanhua_base6');
         
         v_sql := 'create table tmp_o2o_hhb_zhuanhua_base6 nologging as
select jifen_month,cbss_developer_id,count(1) fenmu 
from tmp_o2o_hhb_zhuanhua_base1
group by jifen_month,cbss_developer_id
               
         ';  
         
         
         execute immediate v_sql ;
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_hhb_zhuanhua_base6 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         safely_drop('tmp_o2o_hhb_zhuanhua_base7');
         
         v_sql := 'create table tmp_o2o_hhb_zhuanhua_base7 nologging as
select jifen_month,develop_depart_to_grid_id,develop_depart_to_grid_name,count(1) fenzi 
from tmp_o2o_hhb_zhuanhua_base4
group by jifen_month,develop_depart_to_grid_id,develop_depart_to_grid_name
               
         ';  
         
         
         execute immediate v_sql ;
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_hhb_zhuanhua_base7 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         safely_drop('tmp_o2o_hhb_zhuanhua_base8');
         
         v_sql := 'create table tmp_o2o_hhb_zhuanhua_base8 nologging as
select jifen_month,develop_depart_to_grid_id,develop_depart_to_grid_name,count(1) fenmu 
from tmp_o2o_hhb_zhuanhua_base1
group by jifen_month,develop_depart_to_grid_id,develop_depart_to_grid_name
         ';  
         
         
         execute immediate v_sql ;
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_hhb_zhuanhua_base8 OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         safely_drop('tmp_o2o_hhb_zhuanhua_person');
         
         v_sql := 'create table tmp_o2o_hhb_zhuanhua_person nologging as
select a.jifen_month,a.cbss_developer_id,
(case when nvl(a.fenmu,0) =0 then 0 else round(nvl(b.fenzi,0)/nvl(a.fenmu,0),2) end) 配送员转化率 
from tmp_o2o_hhb_zhuanhua_base6 a,tmp_o2o_hhb_zhuanhua_base5 b
where a.cbss_developer_id=b.cbss_developer_id(+)

         ';  
         
         
         execute immediate v_sql ;
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_hhb_zhuanhua_person OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
          safely_drop('tmp_o2o_hhb_zhuanhua_group');
         
         v_sql := 'create table tmp_o2o_hhb_zhuanhua_group nologging as
select a.jifen_month,a.develop_depart_to_grid_id,a.develop_depart_to_grid_name,
(case when nvl(a.fenmu,0) =0 then 0 else round(nvl(b.fenzi,0)/nvl(a.fenmu,0),2) end) 厅经理转化率 
from tmp_o2o_hhb_zhuanhua_base8 a,tmp_o2o_hhb_zhuanhua_base7 b
where a.develop_depart_to_grid_id=b.develop_depart_to_grid_id(+)

         ';  
         
         
         execute immediate v_sql ;
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'create tmp_o2o_hhb_zhuanhua_group OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         
         ---加入备份表
         

          v_sql := 'delete from tmp_o2o_hhb_zhuanhua_mingxi_bk 
           where jifen_month = ''' || p_month || '''                       
            '; 
                      
          execute immediate v_sql ;
          commit;
         
         
         
         v_sql := 'insert into tmp_o2o_hhb_zhuanhua_mingxi_bk 
        select a.JIFEN_MONTH,a.SERIAL_NUMBER,a.PRODUCT_NAME,
a.USER_STATE_NAME,a.IN_DATE,a.DEVELOP_DEPART_TO_GRID_NAME,
a.DEVELOP_DEPART_TO_GRID_ID,a.CBSS_DEVELOPER_ID,a.USER_ID,b.first_fee,b.first_recv_time 
from tmp_o2o_hhb_zhuanhua_base2 a,tmp_o2o_hhb_zhuanhua_base4 b
where a.user_id=b.user_id(+) ';
         
         execute immediate v_sql ;
         commit;
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'insert tmp_o2o_hhb_zhuanhua_mingxi_bk OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         
         --
         
          v_sql := 'delete from tmp_o2o_hhb_peisong_mingxi_bk 
           where jifen_month = ''' || p_month || '''                       
            '; 
                      
          execute immediate v_sql ;
          commit;
         
         
         
         v_sql := 'insert into tmp_o2o_hhb_peisong_mingxi_bk 
       select a.*,b.bss_product_name product_name_b,b.user_state_name user_state_b,b.develop_depart_to_region_name
from tmp_o2o_peisong_yx_2 a,report.t_user_all_' || p_month || ' b
where a.user_id_b=b.user_id
and b.bss_product_name like ''%亲情卡%'' ';
         
         execute immediate v_sql ;
         commit;
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'insert tmp_o2o_hhb_peisong_mingxi_bk OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         --
         
         v_sql := 'delete from tmp_o2o_hhb_zhuanhua_person_bk 
           where jifen_month = ''' || p_month || '''                       
            '; 
                      
          execute immediate v_sql ;
          commit;
         
         
         
         v_sql := 'insert into tmp_o2o_hhb_zhuanhua_person_bk 
       select * from  tmp_o2o_hhb_zhuanhua_person';
         
         execute immediate v_sql ;
         commit;
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'insert tmp_o2o_hhb_zhuanhua_person_bk OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         --
         v_sql := 'delete from tmp_o2o_hhb_zhuanhua_group_bk 
           where jifen_month = ''' || p_month || '''                       
            '; 
                      
          execute immediate v_sql ;
          commit;
         
         
         
         v_sql := 'insert into tmp_o2o_hhb_zhuanhua_group_bk 
       select * from  tmp_o2o_hhb_zhuanhua_group';
         
         execute immediate v_sql ;
         commit;
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'insert tmp_o2o_hhb_zhuanhua_group_bk OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
         --
         v_sql := 'delete from tmp_o2o_hhb_peisong_bk
           where jifen_month = ''' || p_month || '''                       
            '; 
                      
          execute immediate v_sql ;
          commit;
         
          v_sql := 'insert into tmp_o2o_hhb_peisong_bk 
       select * from  tmp_o2o_peisong_yx_4';
         
         execute immediate v_sql ;
         commit;
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'insert tmp_o2o_hhb_peisong_bk OK  ';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ;
      
    
       exception 
         when others then 
           
          v_sql := 'delete from tmp_o2o_hhb_zhuanhua_mingxi_bk 
           where jifen_month = ''' || p_month || '''                       
            '; 
                      
          execute immediate v_sql ;
          commit;
          
           v_sql := 'delete from tmp_o2o_hhb_peisong_mingxi_bk 
           where jifen_month = ''' || p_month || '''                       
            '; 
                      
          execute immediate v_sql ;
          commit;
          
          v_sql := 'delete from tmp_o2o_hhb_zhuanhua_person_bk 
           where jifen_month = ''' || p_month || '''                       
            '; 
                      
          execute immediate v_sql ;
          commit;
          
          v_sql := 'delete from tmp_o2o_hhb_zhuanhua_group_bk 
           where jifen_month = ''' || p_month || '''                       
            '; 
                      
          execute immediate v_sql ;
          commit;
          
          v_sql := 'delete from tmp_o2o_hhb_peisong_bk
           where jifen_month = ''' || p_month || '''                       
            '; 
                      
          execute immediate v_sql ;
          commit;
                    
           
           v_run_order := v_run_order + 1 ;         
           v_end_flag :=  0;
           v_remark := sqlerrm ;
           insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark);
           commit ;
           
      
    end p_o2o_peisong_yy;
    
   
    procedure p_o2o_run_all(p_month in varchar2) is
      
         v_start_date date ; 
         v_pkg_name varchar2(50);
         v_prg_name varchar2(50);
         v_run_order number(5);
         v_remark varchar2(300);
         v_end_flag number(1);

     begin

--log    
         v_start_date := sysdate ;
         v_pkg_name := 'pkg_chnl_o2o_data'; 
         v_prg_name := 'p_o2o_run_all' ;
         v_run_order := 0 ;  
        
         IF p_month IS  NULL or length(p_month) <> 6 THEN
            raise_application_error(-20001,'p_month is error');
         END IF ;      
         
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'RUN start All :';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,v_start_date,sysdate,v_remark) ;
         commit ; 
         
         p_o2o_prepare_data(p_month) ;
      
         p_o2o_cuxiao_sh(p_month);
     
         p_o2o_dudao_sh(p_month); 
     
        ---p_o2o_xiangmu_sh(p_month);   
     
         p_o2o_ditui_zq(p_month);     
          
         p_o2o_dudao_zq(p_month); 
        
     
         p_o2o_jianzhi_zq(p_month); 
         
         --p_o2o_lianmeng_sh(p_month);
         
         --p_o2o_zhongyou_sh(p_month);
         
         p_o2o_wangka_yy(p_month);
         
         p_o2o_qinqingka_yy(p_month);
         
         p_o2o_dashi_yy(p_month);
         -- p_o2o_peisong_yy(p_month);
                
     
         v_run_order := v_run_order + 1 ;
         v_end_flag := 1 ;
         v_remark := 'RUN end All:';
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,sysdate,sysdate,v_remark) ;
         commit ;
         
      exception 
       when others then     
         v_run_order := v_run_order + 1 ;         
         v_end_flag :=  0;
         v_remark := sqlerrm ;
         insert into log_prog_run_data values(v_pkg_name,v_prg_name,v_run_order,v_end_flag,sysdate,sysdate,v_remark);
         commit ; 
    
    
    
    end p_o2o_run_all ;

begin
  -- Initialization
  null;
end pkg_chnl_o2o_data;
/
