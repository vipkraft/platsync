--1 -- Транзит отправление
 -- insert into av_trip 
select *,                                                               
      CASE WHEN h.ot_id_point = 1229                                         
           THEN 1                                                             
           WHEN h.do_id_point = 1229                                              
           THEN 2                                                                   
           WHEN (h.ot_id_point<>1229 and h.do_id_point<>1229)                              
           THEN 1                                                                         
           END AS napr,                                                                       
1229 as id_point_server                                                                        
    from                                                                                        
    (select a.id_shedule,                                                                          
           a.plat_o as plat,                   
  (select n.active from av_shedule n where n.del=0 and n.id=a.id_shedule order by n.del asc,n.createdate desc limit 1)
          as active,
(select n.dateactive from av_shedule n where n.del=0 and n.id=a.id_shedule order by n.del asc,n.createdate desc limit 1)
          as dateactive,
(select n.dates from av_shedule n where n.del=0 and n.id=a.id_shedule order by n.del asc,n.createdate desc limit 1)
          as dates,
  (select n.datepo from av_shedule n where n.del=0 and n.id=a.id_shedule order by n.del asc,n.createdate desc limit 1)
          as datepo,
       (select j.id_point                            
          from av_shedule_sostav j                      
           where                                           
               (j.del=0) and                                  
               j.id_shedule=a.id_shedule and                     
               j.form=1 and                                         
               j.point_order<a.point_order                             
               order by j.point_order DESC limit 1                         
               )                                                             
          as ot_id_point,                                                       
       (select k.point_order                                                             
          from av_shedule_sostav k                                                          
           where                                                                               
               (k.del=0) and                                                                      
               k.id_shedule=a.id_shedule and                                                         
               k.form=1 and                          
               k.point_order<a.point_order              
               order by k.point_order DESC limit 1          
               )                                              
          as ot_order,                                           
       (select l.name                                                     
          from av_shedule_sostav l                                           
           where                                                                
               (l.del=0) and                                                       
               l.id_shedule=a.id_shedule and                                          
               l.form=1 and                                                              
               l.point_order<a.point_order                                                  
               order by l.point_order DESC limit 1                                              
               )                                                                                  
          as ot_name,                                                                                
       (select b.id_point               
          from av_shedule_sostav b         
           where                              
               (b.del=0) and                     
               b.id_shedule=a.id_shedule and        
               b.form=1 and                            
               b.point_order>a.point_order                
               order by b.point_order ASC limit 1            
               )                                                
          as do_id_point,                                          
       (select c.point_order                                                
          from av_shedule_sostav c                                             
           where                                                                  
               (c.del=0) and                                                         
               c.id_shedule=a.id_shedule and                                            
               c.form=1 and                                                                
               c.point_order>a.point_order                                                    
               order by c.point_order ASC limit 1                                                
               )                                                                                    
          as do_order,                                                                                 
       (select d.name                                        
          from av_shedule_sostav d                              
           where                                                   
               (d.del=0) and                                          
               d.id_shedule=a.id_shedule and                             
               d.form=1 and                                                 
               d.point_order>a.point_order                                     
               order by d.point_order ASC limit 1                                 
               )                                                                     
          as do_name,                                                                   
          a.form,                                                                             
          a.t_o,                                                                                 
          a.t_s,                                                                                    
          a.t_p,                                                                                       
          a.t_d,                 
       (select zakaz  
          from av_shedule
           where del=0 and id=a.id_shedule order by createdate desc limit 1
          ) as zakaz,                                
       (select date_tarif                           
          from av_shedule
           where del=0 and id=a.id_shedule order by createdate desc limit 1
         ) as date_tarif,                                                         
       (select id_route                                                           
          from av_shedule
           where del=0 and id=a.id_shedule order by createdate desc limit 1
        ) as id_route                                                                                            
   from av_shedule_sostav a    
   where                          
         (a.del=0) and               
         a.id_shedule IN                
         (select z.id                      
          from av_shedule z                   
          where                                  
                z.del=0 and                         
                z.datepo>=current_date                
         )                                                         
         and                                                          
         a.point_order not in                                            
         (select max(l.point_order)                                         
         from av_shedule_sostav l                                              
         where                                                                    
         l.del=0 and                                                                 
         l.id_shedule=a.id_shedule)                                                     
          and                                                                              
         a.id_point=1229 and a.form=0) h                                                                     
 where            
h.ot_id_point>0 and h.ot_id_point<>h.do_id_point
     order by h.id_shedule;   

     
 --2 -- Транзит прибытие    
 -- insert into av_trip 
select *,                                                               
      CASE WHEN h.ot_id_point = 1229                                         
           THEN 1                                                             
           WHEN h.do_id_point = 1229                                              
           THEN 2                                                                   
           WHEN (h.ot_id_point<>1229 and h.do_id_point<>1229)                              
           THEN 2                                                                         
           END AS napr,                                                                       
1229 as id_point_server                                                                        
    from                                                                                        
    (select a.id_shedule,                                                                          
           a.plat_o as plat,                   
  (select n.active from av_shedule n where n.del=0 and n.id=a.id_shedule order by n.del asc,n.createdate desc limit 1)
          as active,
(select n.dateactive from av_shedule n where n.del=0 and n.id=a.id_shedule order by n.del asc,n.createdate desc limit 1)
          as dateactive,
(select n.dates from av_shedule n where n.del=0 and n.id=a.id_shedule order by n.del asc,n.createdate desc limit 1)
          as dates,
  (select n.datepo from av_shedule n where n.del=0 and n.id=a.id_shedule order by n.del asc,n.createdate desc limit 1)
          as datepo,
       (select j.id_point                            
          from av_shedule_sostav j                      
           where                                           
               (j.del=0) and                                  
               j.id_shedule=a.id_shedule and                     
               j.form=1 and                                         
               j.point_order<a.point_order                             
               order by j.point_order DESC limit 1                         
               )                                                             
          as ot_id_point,                                                       
       (select k.point_order                                                             
          from av_shedule_sostav k                                                          
           where                                                                               
               (k.del=0) and                                                                      
               k.id_shedule=a.id_shedule and                                                         
               k.form=1 and                          
               k.point_order<a.point_order              
               order by k.point_order DESC limit 1          
               )                                              
          as ot_order,                                           
       (select l.name                                                     
          from av_shedule_sostav l                                           
           where                                                                
               (l.del=0) and                                                       
               l.id_shedule=a.id_shedule and                                          
               l.form=1 and                                                              
               l.point_order<a.point_order                                                  
               order by l.point_order DESC limit 1                                              
               )                                                                                  
          as ot_name,                                                                                
       (select b.id_point               
          from av_shedule_sostav b         
           where                              
               (b.del=0) and                     
               b.id_shedule=a.id_shedule and        
               b.form=1 and                            
               b.point_order>a.point_order                
               order by b.point_order ASC limit 1            
               )                                                
          as do_id_point,                                          
       (select c.point_order                                                
          from av_shedule_sostav c                                             
           where                                                                  
               (c.del=0) and                                                         
               c.id_shedule=a.id_shedule and                                            
               c.form=1 and                                                                
               c.point_order>a.point_order                                                    
               order by c.point_order ASC limit 1                                                
               )                                                                                    
          as do_order,                                                                                 
       (select d.name                                        
          from av_shedule_sostav d                              
           where                                                   
               (d.del=0) and                                          
               d.id_shedule=a.id_shedule and                             
               d.form=1 and                                                 
               d.point_order>a.point_order                                     
               order by d.point_order ASC limit 1                                 
               )                                                                     
          as do_name,                                                                   
          a.form,                                                                             
          a.t_o,                                                                                 
          a.t_s,                                                                                    
          a.t_p,                                                                                       
          a.t_d,                                                                                       
       (select zakaz  
          from av_shedule
           where del=0 and id=a.id_shedule order by createdate desc limit 1
          ) as zakaz,                                
       (select date_tarif                           
          from av_shedule
           where del=0 and id=a.id_shedule order by createdate desc limit 1
         ) as date_tarif,                                                         
       (select id_route                                                           
          from av_shedule
           where del=0 and id=a.id_shedule order by createdate desc limit 1
        ) as id_route                                                                                            
   from av_shedule_sostav a    
   where                          
         (a.del=0) and               
         a.id_shedule IN                
         (select z.id                      
          from av_shedule z                   
          where                                  
                z.del=0 and                         
                z.datepo>=current_date                
         )                                                         
         and                                                          
         a.point_order not in                                            
         (select max(l.point_order)                                         
         from av_shedule_sostav l                                              
         where                                                                    
         l.del=0 and                                                                 
         l.id_shedule=a.id_shedule)                                                     
          and                                                                              
         a.id_point=1229 and a.form=0) h                                                                     
 where            
h.ot_id_point>0 and h.ot_id_point<>h.do_id_point
     order by h.id_shedule;   
     
--3 -- Формирующиеся отправление     
 --- insert into av_trip 
select *,                                                                   
      CASE WHEN h.ot_id_point = 1229
           THEN 1                                                                 
           WHEN h.do_id_point = 1229
           THEN 2                                                                       
           WHEN (h.ot_id_point<>1229 and h.do_id_point<>1229)
           THEN 1     
           END AS napr,                                                                       
1229 as id_point_server                                                                        
    from                    
    (select a.id_shedule,      
           a.plat_o as plat,      
  (select n.active from av_shedule n where n.del=0 and n.id=a.id_shedule order by n.del asc,n.createdate desc limit 1)
          as active,
(select n.dateactive from av_shedule n where n.del=0 and n.id=a.id_shedule order by n.del asc,n.createdate desc limit 1)
          as dateactive,
(select n.dates from av_shedule n where n.del=0 and n.id=a.id_shedule order by n.del asc,n.createdate desc limit 1)
          as dates,
  (select n.datepo from av_shedule n where n.del=0 and n.id=a.id_shedule order by n.del asc,n.createdate desc limit 1)
          as datepo,
       (select j.id_point               
          from av_shedule_sostav j         
           where                              
               (j.del=0) and                     
               j.id_shedule=a.id_shedule and        
               j.form=1 and                            
               j.point_order=a.point_order                
               order by j.point_order ASC limit 1         
               )                                             
          as ot_id_point,                                       
       (select k.point_order                                             
          from av_shedule_sostav k                                          
           where                                                               
               (k.del=0) and                                                      
               k.id_shedule=a.id_shedule and                                         
               k.form=1 and                                                             
               k.point_order=a.point_order                                                 
               order by k.point_order ASC limit 1         
               )                                                                              
          as ot_order,                                                                           
       (select l.name                                                                                     
          from av_shedule_sostav l  
           where                       
               (l.del=0) and              
               l.id_shedule=a.id_shedule and 
               l.form=1 and                     
               l.point_order=a.point_order         
               order by l.point_order ASC limit 1         
               )                                      
          as ot_name,                                    
       (select b.id_point                                      
          from av_shedule_sostav b                                
           where                                                     
               (b.del=0) and                                            
               b.id_shedule=a.id_shedule and                               
               b.form=1 and                                                   
               b.point_order>a.point_order                                       
               order by b.point_order ASC limit 1                                   
               )                                                                       
          as do_id_point,                                                                 
       (select c.point_order                                    
          from av_shedule_sostav c                                 
           where                                                      
               (c.del=0) and                                             
               c.id_shedule=a.id_shedule and                                
               c.form=1 and                                                    
               c.point_order>a.point_order                                        
               order by c.point_order ASC limit 1                                    
               )                                                                        
          as do_order,                                                                     
       (select d.name                                    
          from av_shedule_sostav d                          
           where                                               
               (d.del=0) and                                      
               d.id_shedule=a.id_shedule and                         
               d.form=1 and                                             
               d.point_order>a.point_order                                 
               order by d.point_order ASC limit 1                             
               )                                                                 
          as do_name,                                                               
          a.form,                                                                         
          a.t_o,   
          a.t_s,      
          a.t_p,         
          a.t_d,         
       (select zakaz  
          from av_shedule
           where del=0 and id=a.id_shedule order by createdate desc limit 1
          ) as zakaz,                                
       (select date_tarif                           
          from av_shedule
           where del=0 and id=a.id_shedule order by createdate desc limit 1
         ) as date_tarif,                                                         
       (select id_route                                                           
          from av_shedule
           where del=0 and id=a.id_shedule order by createdate desc limit 1
        ) as id_route                              
   from av_shedule_sostav a                                                            
   where                                                                                  
         (a.del=0) and                                                                       
         a.id_shedule IN          
         (select z.id                
          from av_shedule z             
          where                            
                z.del=0 and                   
                z.datepo>=current_date 
             --    and z.id=2644          
         )                                                   
         and                                                    
         a.point_order not in                                      
         (select max(l.point_order)                                   
         from av_shedule_sostav l                                        
         where                                                              
         l.del=0 and                                                           
         l.id_shedule=a.id_shedule)                                               
          and                                                                        
         (a.id_point=1229 and a.form=1)) h                                         
where 
      h.ot_id_point>0 
     order by h.id_shedule;                         
     
--4 -- Формирующиеся прибытие     
 -- insert into av_trip 
select *,                                                                  
      CASE WHEN h.ot_id_point = 1229
           THEN 1                                                                  
           WHEN h.do_id_point = 1229
           THEN 2                                                                          
           WHEN (h.ot_id_point<>1229 and h.do_id_point<>1229)
           THEN 1   
           END AS napr,                                                                       
1229 as id_point_server                                                                        
    from                    
    (select a.id_shedule,       
           a.plat_o as plat,        
  (select n.active from av_shedule n where n.del=0 and n.id=a.id_shedule order by n.del asc,n.createdate desc limit 1)
          as active,
(select n.dateactive from av_shedule n where n.del=0 and n.id=a.id_shedule order by n.del asc,n.createdate desc limit 1)
          as dateactive,
(select n.dates from av_shedule n where n.del=0 and n.id=a.id_shedule order by n.del asc,n.createdate desc limit 1)
          as dates,
  (select n.datepo from av_shedule n where n.del=0 and n.id=a.id_shedule order by n.del asc,n.createdate desc limit 1)
          as datepo,
       (select j.id_point                   
          from av_shedule_sostav j              
           where                                    
               (j.del=0) and                            
               j.id_shedule=a.id_shedule and                
               j.form=1 and                                     
               j.point_order<a.point_order                          
               order by j.point_order DESC limit 1                      
               )                                                            
          as ot_id_point,                                                       
       (select k.point_order                                                                
          from av_shedule_sostav k                                                              
           where                                                                                    
               (k.del=0) and                                                                            
               k.id_shedule=a.id_shedule and  
               k.form=1 and                       
               k.point_order<a.point_order            
               order by k.point_order DESC limit 1        
               )                                              
          as ot_order,                                            
       (select l.name                                                         
          from av_shedule_sostav l                                                
           where                                                                      
               (l.del=0) and                                                              
               l.id_shedule=a.id_shedule and                                                  
               l.form=1 and                                                                       
               l.point_order<a.point_order                                                            
               order by l.point_order DESC limit 1                                                        
               ) 
          as ot_name, 
       (select b.id_point               
          from av_shedule_sostav b          
           where                                
               (b.del=0) and                        
               b.id_shedule=a.id_shedule and            
               b.form=1 and                                 
               b.point_order=a.point_order limit 1                      
               )                                                    
          as do_id_point,                                               
       (select c.point_order                                                        
          from av_shedule_sostav c                                                      
           where                                                                            
               (c.del=0) and                                                                    
               c.id_shedule=a.id_shedule and                                                        
               c.form=1 and                                                                             
               c.point_order=a.point_order limit 1                                                                 
               ) 
          as do_order, 
       (select d.name                                    
          from av_shedule_sostav d                           
           where                                                 
               (d.del=0) and                                         
               d.id_shedule=a.id_shedule and                             
               d.form=1 and                                                  
               d.point_order=a.point_order  limit 1                                     
               )                                                                     
          as do_name,                                                                    
          a.form,                                                                                
          a.t_o,                                                                                     
          a.t_s,                                                                                         
          a.t_p,                                                                                             
          a.t_d,                                                                                             
       (select zakaz  
          from av_shedule
           where del=0 and id=a.id_shedule order by createdate desc limit 1
          ) as zakaz,                                
       (select date_tarif                           
          from av_shedule
           where del=0 and id=a.id_shedule order by createdate desc limit 1
         ) as date_tarif,                                                         
       (select id_route                                                           
          from av_shedule
           where del=0 and id=a.id_shedule order by createdate desc limit 1
        ) as id_route                                                                                            
   from av_shedule_sostav a                                                                       
   where                                                                                              
         (a.del=0) and                                                                                    
         a.id_shedule IN                                                                                      
         (select z.id                                                                                             
          from av_shedule z  
          where                  
                z.del=0 and          
                z.datepo>=current_date    
         )                                                
                                                           
    and  a.point_order>1                                                                     
    and (a.id_point=1229 and a.form=1)) h                                                      
where 
     h.do_id_point>0 and h.ot_id_point>0
     order by h.id_shedule;                                                                               

---------- 5 -----------     
     
-- insert into av_trip_atp_ats 
select z.*  from 
(select a.id_shedule,                   
        a.id_kontr,                         
       (select c.name                         
        from av_spr_kontragent c                 
        where c.del=0 and c.id=a.id_kontr order by c.createdate desc limit 1 
       ) as name_kontr,                                
        a.def_ats as id_ats,                               
       (select (trim(c.name)||' ГН: '||trim(c.gos))            
       from av_spr_ats c                                        
       where c.del=0 and c.id=a.def_ats order by c.createdate desc limit 1 
       ) as name_ats,                                                  
       (select c.type_ats            
       from av_spr_ats c                                        
       where c.del=0 and c.id=a.def_ats  order by c.createdate desc limit 1
       ) as type_ats,                                                  
        (select (c.m_lay+c.m_lay_two+c.m_down+c.m_down_two)    
         from av_spr_ats c                                           
         where c.del=0 and c.id=a.def_ats  order by c.createdate desc limit 1
         ) as all_mest,                                                     
        (select c.comfort    
         from av_spr_ats c                                           
         where c.del=0 and c.id=a.def_ats  order by c.createdate desc limit 1 
         ) as confort,                                                     
       (select f.massezon                       
        from av_shedule_sezon f                 
        where f.del=0 and                          
              f.id_shedule=a.id_shedule and           
              f.id_kontr=a.id_kontr  order by f.createdate desc limit 1  
       ) as sezon                                     
from av_shedule_atp a                                                    
where a.del=0 and     
      a.def_ats<>0 and                                             
      a.id_shedule in (select distinct r.id_shedule from av_shedule_sostav r where r.del=0 and r.id_point=1229)  
order by a.id_shedule) z 
where not trim(z.name_ats)='';

----------- 6 ------------------------
-- insert into av_trip_dog_lic 
select b.id_kontr,             
       b.datazak as dates,        
       case when (b.datapog is null) or (b.datapog < '01.01.1900') then b.datazak else b.datapog end as datepo,        
       1 as type_date                   
from av_spr_kontr_dog b                    
where                                         
  b.del=0 and                                    
  substr(trim(b.viddog),length(trim(b.viddog)),1)='2' and                        
  b.id_kontr in                                        
  (select distinct(a.id_kontr) from av_trip_atp_ats a     
  );                                                         
-- insert into av_trip_dog_lic 
select c.id_kontr,              
       c.datanach as dates,      
       case when (c.dataok is null) or (c.dataok < '01.01.1900') then '01.10.2100' else c.dataok end as datepo,        
       2 as type_date                
from av_spr_kontr_license c                
where                                         
  c.del=0 and                                    
  c.id_kontr in                                     
  (select distinct(a.id_kontr) from av_trip_atp_ats a  
  );                                                      
