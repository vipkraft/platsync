select z.zag||' '||z.src as src from 
 (SELECT 'CREATE OR REPLACE '||p.proname||'('||pg_catalog.pg_get_function_arguments(p.oid)||')'||' RETURNS '||' '||pg_catalog.pg_get_function_result(p.oid)||' AS $BODY$ ' as zag,
 (SELECT  a.prosrc||' '||'$BODY$ LANGUAGE plpgsql VOLATILE  COST 100;'  FROM  pg_catalog.pg_proc a,pg_catalog.pg_namespace b WHERE  a.proname=p.proname and b.nspowner=a.proowner and b.nspname='public'    
 ) as src
 FROM pg_catalog.pg_proc p
      LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
 WHERE p.proname ~ '^(gettarif)$'
       AND pg_catalog.pg_function_is_visible(p.oid)) z;


--select  * from pg_namespace;
--select  * from pg_proc;

--SELECT  a.prosrc FROM  pg_catalog.pg_proc a,pg_catalog.pg_namespace b WHERE  a.proname not LIKE '%dblink%' and b.nspowner=a.proowner and b.nspname='public';    
--JOIN    pg_catalog.pg_proc p ON      pronamespace = n.oid WHERE   nspname = 'public' and proname not LIKE '%dblink%';    


--$BODY$
--  LANGUAGE plpgsql VOLATILE
--  COST 100;