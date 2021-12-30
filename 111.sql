CREATE OR REPLACE FUNCTION aflawless_dohod(
    refcursor,
    date1 date,
    date2 date,
    idlocal integer)
  RETURNS refcursor AS
$BODY$
DECLARE 
 sum numeric(12,2)=0;
 tick numeric(12,2)=0;
 bags numeric(12,2)=0;
 cred numeric(12,2)=0;
 komsbor numeric(12,2)=0;
 n1525 numeric(12,2)=0;
 
 myrec record;

BEGIN

  FOR myrec IN SELECT a.tarif_calculated,a.sum_cash,a.sum_credit,a.type_ticket FROM av_ticket a 
               where a.trip_date>=date1 AND a.trip_date<=date2
               AND a.type_oper=1
               AND a.id_ot=idlocal AND a.id_user>0
 AND (a.unused=1 OR (SELECT c.trip_flag FROM av_disp_oper c WHERE c.del=0 AND c.id_shedule=a.id_shedule AND c.trip_date=a.trip_date 
 AND c.trip_date<(date2+interval '1 days')
 AND c.trip_time=a.trip_time AND c.point_order=a.order_trip_ot order by c.createdate DESC limit 1)>4 
 OR (case when type_ticket=2 then 
 case when not((SELECT b.ticket_num FROM av_ticket b WHERE not(trim(b.ticket_num)='') and trim(b.bagage_num)='' AND b.trip_date<(date2+interval '1 days')
 AND b.ticket_num=a.ticket_num AND (b.type_oper=2 or b.unused=1))='') then true else false end 
 else false END)) 

 LOOP
    IF myrec.sum_credit=0 AND myrec.type_ticket=1 THEN tick:=tick + myrec.tarif_calculated;
                                                              sum:=sum+myrec.tarif_calculated;
     END IF;  
    IF myrec.type_ticket=2 THEN bags:=bags + myrec.sum_cash;
                                    sum:=sum+myrec.sum_cash;
     END IF;  
    IF myrec.sum_credit>0 AND myrec.type_ticket=1 THEN cred:=cred + myrec.tarif_calculated; 
                                                              sum:=sum+myrec.tarif_calculated;
     END IF; 
     
 END LOOP;
     


--ком сбор
 SELECT sum(substr(a.uslugi_text,position('[u5]' in a.uslugi_text)+5,position('|' in trim(leading '[u5]|' from a.uslugi_text))-1)::numeric) into komsbor
 FROM av_ticket a
 where position('[u5]|' in a.uslugi_text)>0 and
   a.type_ticket=1 and                
  date(a.createdate)>=date1 and date(a.createdate)<=date2
  and a.id_point_oper=idlocal;

sum:=sum + komsbor;  


FOR myrec IN SELECT a.tarif_calculated,a.refund_sum,a.sum_credit,a.refund_percent FROM av_ticket a
   where (a.type_oper=2) and a.id_user>0
         and not(a.refund_percent=0)
         and not(a.refund_percent=100)
         and date(a.refund_createdate)>=date1 and date(a.refund_createdate)<=date2     
         and a.refund_id_point=idlocal
Loop
    IF myrec.sum_credit=0 THEN n1525:=n1525 + myrec.tarif_calculated-myrec.refund_sum;
                                               sum:=sum+myrec.tarif_calculated-myrec.refund_sum;
     END IF;  
    IF myrec.sum_credit>0 THEN n1525:=n1525 + round(myrec.tarif_calculated*myrec.refund_percent*0.01,2);
                                               sum:=sum+round(myrec.tarif_calculated*myrec.refund_percent*0.01,2);
     END IF;  
END LOOP;

FOR myrec IN SELECT a.tarif_calculated,a.refund_sum,a.sum_credit,a.refund_percent FROM av_ticket_local a
   where (a.type_oper=2) and a.id_user>0
         and not(a.refund_percent=0)
         and not(a.refund_percent=100)
         and date(a.refund_createdate)>=date1 and date(a.refund_createdate)<=date2     
         and a.refund_id_point=idlocal
Loop
    IF myrec.sum_credit=0 THEN n1525:=n1525 + myrec.tarif_calculated-myrec.refund_sum;
                                               sum:=sum+myrec.tarif_calculated-myrec.refund_sum;
     END IF;  
    IF myrec.sum_credit>0 THEN n1525:=n1525 + round(myrec.tarif_calculated*myrec.refund_percent*0.01,2);
                                               sum:=sum+round(myrec.tarif_calculated*myrec.refund_percent*0.01,2);
     END IF;  
END LOOP;

open $1 for
  select sum, tick, bags, cred, komsbor, n1525;
return $1;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION aflawless_dohod(refcursor, date, date, integer)
  OWNER TO platforma;