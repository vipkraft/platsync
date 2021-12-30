--select z.zag||' '||z.src as src from 
select 
 ('CREATE OR REPLACE '||p.proname||'('||pg_catalog.pg_get_function_arguments(p.oid)||')'||' RETURNS '||' '||pg_catalog.pg_get_function_result(p.oid)||' AS $BODY$ ') as zag
 ,p.prosrc
 ,('$BODY$ LANGUAGE plpgsql VOLATILE COST 100; ALTER FUNCTION '||p.proname||'('||pg_catalog.pg_get_function_arguments(p.oid)||')'||' OWNER TO platforma;') fin
 FROM pg_catalog.pg_proc p
   --   LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
 WHERE pronamespace=2200
 --p.proname ~ '^(gettarif)$'       
     AND pg_catalog.pg_function_is_visible(p.oid)       
     and p.proname !~ '^dblink'    
     --   order by proname
     --  ) ;
