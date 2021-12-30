CREATE TABLE av_servers_access_new2 AS SELECT distinct on (server_id, destination_id, id_user,del,createdate_first,id_user_first)
       server_id, destination_id, id_user, createdate, del, id_user_first, 
       createdate_first
FROM av_servers_access_new
where del<3
order by server_id,destination_id;