unit sync_proc;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils,forms,
  dialogs,//ZConnection,
  ZDataset
  //,strutils
  ,db;

// Обновлениле хранимых процедур
function updproc(nameproc:string;idserver:integer):integer;



implementation
uses
  main,platproc;

// Удаление процедур/процедуры по имени
function updproc(nameproc:string;idserver:integer):integer;
 var
   BlobStreamIZ: TStream;
   BlobStreamW: TStream;
   FileStream : TFileStream;
   n:integer;
   tekflag:byte=0;
begin

  form1.write_log('=============================update_proc========================================');
  form1.write_log('=================СИНХРОНИЗАЦИЯ ХРАНИМЫХ ПРОЦЕДУР=========================');
  form1.write_log('==========СЕРВЕР: ['+connectini[14]+'] =======================');

  // ================Проверяем доступность серверов======================
  form1.write_log('*********** Определение доступности Центрального и Локального серверов');

  // Подключаемся к центральному серверу
 If not(Connect2(form1.Zconnection1, 1)) then
    begin
      form1.write_log('- Центральный сервер:  - НЕТ СОЕДИНЕНИЯ');

      tekflag:=1;
    end;

  // Подключаемся к локальному серверу
   If not(Connect2(form1.Zconnection2, 2)) then
    begin
      form1.write_log('- Локальный сервер: - НЕТ СОЕДИНЕНИЯ');

      tekflag:=1;
    end;


  if tekflag=1 then
    begin
     form1.write_log('!!! ОСТАНОВКА !!!  !!! НЕТ СОЕДИНЕНИЯ C СЕРВЕРАМИ!!!');

     form1.ZConnection2.Disconnect;
     form1.ZConnection1.Disconnect;
     Result:=1;
     exit;
    end;



  // УДАЛЯЕМ ТЕКУЩУЮ ХРАНИМКУ С ЛОКАЛЬНОГО СЕРВЕРА ПО МАСКЕ
  //
  form1.write_log('-- Запрос на наличие процедуры '+nameproc+' на локальном сервере...');
  form1.ZQuery2.SQL.Clear;
  form1.ZQuery2.SQL.Add('  select ');
  form1.ZQuery2.SQL.Add(' (''DROP FUNCTION ''||p.proname||''(''||pg_catalog.pg_get_function_arguments(p.oid)||'');'') as ddd ');
  form1.ZQuery2.SQL.Add(' FROM pg_catalog.pg_proc p ');
  form1.ZQuery2.SQL.Add(' WHERE p.proname ~ ''^'+nameproc+''' ');
  //showmessage(form1.ZQuery2.SQL.Text);//$
    try
     form1.ZQuery2.open;
    except
        form1.write_log('!!! Ошибка запроса удаления х.процедуры !!!'+#13+ form1.ZQuery2.SQL.Text);
        form1.ZQuery2.Close;
        form1.Zconnection1.disconnect;
        form1.Zconnection2.disconnect;

        Result:=1;
        exit;
     end;
     if form1.ZQuery2.RecordCount=0 then
       begin
         form1.write_log('ХРАНИМОЙ ПРОЦЕДУРы ['+nameproc+'] нет на сервере...');
       end;

    form1.ZSQLMonitor1.Active:=true;

      try
                 If not form1.Zconnection2.InTransaction then
                    begin
                      form1.Zconnection2.StartTransaction;
                    end
                 else
                   begin
                     form1.write_log('!!!77 ОСТАНОВКА   Незавершенная транзакция ');

                     form1.ZConnection2.Rollback;
                     Result:=1;
                     exit;
                   end;


     if form1.ZQuery2.RecordCount>0 then
       begin
         form1.write_log('-- Запрос на удаление процедуры '+nameproc+' на локальном сервере...');
          //If trim(form1.ZQuery2.Fields[0].text)<>'' then
            //begin
         form1.ZQuery3.Connection:=form1.ZConnection2;
         form1.ZQuery3.SQL.Clear;
         form1.ZQuery3.SQL.add(form1.ZQuery2.FieldByName('ddd').AsString);
         //showmessage(form1.ZQuery3.SQL.Text);//$
          try
            form1.ZQuery3.open;
         except
        form1.write_log('!!!2 Ошибка запроса удаления х.процедуры !!!'+#13+ form1.ZQuery3.SQL.Text);
        form1.ZQuery3.Close;
        form1.ZQuery2.Close;
        form1.Zconnection1.disconnect;
        form1.Zconnection2.disconnect;

        Result:=1;
        exit;
        //end;
        end;
       end;
     //form1.ZSQLMonitor1.Active:=true;
  //If 1<>1 then begin

  // ЗАБИРАЕМ ТЕКУШУЮ\ИЕ ХРАНИМКИ С ЦЕНТРАЛЬНОГО СЕРВЕРА
  form1.write_log('-- Забираем текст процедуры '+nameproc+' с центрального сервера...');
  form1.ZQuery1.SQL.Clear;
  form1.ZQuery1.SQL.Add(' select (zag||prosrc||fin) as prosrc FROM ( ');
  form1.ZQuery1.SQL.Add(' select ');
  form1.ZQuery1.SQL.Add('  (SELECT ''CREATE OR REPLACE FUNCTION ''||p.proname||''(''||pg_catalog.pg_get_function_arguments(p.oid)||'')''||'' RETURNS ''||'' ''||pg_catalog.pg_get_function_result(p.oid)||'' AS $BODY$ '') as zag ');
  form1.ZQuery1.SQL.Add('  ,p.prosrc ');
  form1.ZQuery1.SQL.Add('  ,(select ''$BODY$ LANGUAGE plpgsql VOLATILE COST 100; ALTER FUNCTION ''||p.proname||''(''||pg_catalog.pg_get_function_arguments(p.oid)||'')''||'' OWNER TO platforma;'') fin ');
  form1.ZQuery1.SQL.Add('  FROM pg_catalog.pg_proc p ');
//   --   LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
form1.ZQuery1.SQL.Add('  WHERE ');
form1.ZQuery1.SQL.Add(' p.proname ~ ''^'+nameproc+'$'' ) z ');
//form1.ZQuery1.SQL.Add('      AND pg_catalog.pg_function_is_visible(p.oid) ');
//form1.ZQuery1.SQL.Add('      and p.proname !~ ''^dblink'';');
  //showmessage(form1.ZQuery1.SQL.Text);//$
  try
     form1.ZQuery1.open;
   except
        form1.ZQuery1.Close;
        form1.Zconnection1.disconnect;
        form1.Zconnection2.disconnect;
        form1.write_log('!!!3 Ошибка запроса !!!'+#13+ form1.ZQuery3.SQL.Text);

        Result:=1;
        exit;
  end;
      if form1.ZQuery1.RecordCount=0 then
       begin
         form1.ZQuery1.Close;
         form1.Zconnection1.disconnect;
         form1.Zconnection2.disconnect;
         form1.write_log('!!! НЕТ СВЕДЕНИЙ О ХРАНИМОЙ ПРОЦЕДУРЕ: ['+nameproc+'] НА ЦЕНТРАЛЬНОМ СЕРВЕРЕ !!!');

         Result:=0;
         exit;
       end;

  //form1.ZConnection1.Disconnect;//&
  //form1.ZConnection2.Disconnect;//&
  //exit;//&
   form1.write_log('-- Обновляем процедуру '+nameproc+' на локальном сервере...');


        //form1.ZQuery2.SQL.Clear;
        form1.ZSQLProcessor1.Clear;
        //form1.ZSQLProcessor1.LoadFromFile('111.sql');

        //form1.ZSQLProcessor1.Script.Add(form1.ZQuery1.FieldByName('zag').AsString);
        //form1.ZSQLProcessor1.Script.Add(':blob');
        //form1.ZSQLProcessor1.Params.Clear;
        //form1.ZSQLProcessor1.Params.CreateParam(form1.ZQuery1.FieldByName('prosrc').DataType,'blob', ptInput);


  //for n:=0 to form1.ZQuery1.FieldCount-1 do
  //   begin
       //If form1.ZQuery1.Fields[n].Name = 'prosrc' then
       //   showmessage(inttostr(n))
       //   else continue;
       // Устанавливаем параметры
       //form1.ZQuery2.Options:=
       //[doCalcDefaults,doPreferPrepared];
       //[doPreferPrepared];
       // перекидываем через Stream
       // BlobStreamIZ c ЦС
       // Из ЦС
         //If (form1.ZQuery1.Fields[n].DataType=ftMemo) then
          //showmessage(inttostr(n));
          If (form1.ZQuery1.FieldByName('prosrc').DataType=ftBlob) or (form1.ZQuery1.FieldByName('prosrc').DataType=ftMemo) then
                     begin
                            // перекидываем через Stream
                            // BlobStreamIZ c ЦС
                            // Из ЦС
                            //form1.ZQuery2.SQL.add('&blobs'+inttostr(n));


                            //showmessage(form1.ZQuery2.Params[n].ToString);
                            //continue;
                               try
                                 BlobStreamIZ := form1.ZQuery1.CreateBlobStream(form1.ZQuery1.FieldByName('prosrc'), bmRead);
                                 BlobStreamIZ.Position:=0;
                                 //try
                                 //Filestream := Tfilestream.Create('11111'+inttostr(n),fmCreate);
                                 // Filestream.CopyFrom(BlobstreamIZ,BlobStreamIZ.Size);
                                 // finally
                                 //  Filestream.Free;
                                 //  end;

                                  //form1.ZQuery2.ParamByName('blobs'+inttostr(n)).LoadFromStream(BlobStreamIZ,form1.ZQuery1.Fields[n].DataType);
                                  //form1.ZSQLProcessor1.ParamByName('blob').LoadFromStream(BlobStreamIZ,form1.ZQuery1.FieldByName('prosrc').DataType);
                                  form1.ZSQLProcessor1.LoadFromStream(BlobStreamIZ);

                                finally
                                 //if BlobStreamIZ.Size>0 then
                                 BlobStreamIZ.Free;
                               end;
                        end;
                            //showmessage(inttostr( BlobStreamIZ.Size));
                            //form1.ZQuery2.SQL.add(form1.ZQuery1.Fields[n].text);
         //
         //form1.ZQuery2.SQL.add(
         //form1.write_log(form1.ZQuery2.sql.text);//$
         form1.ZSQLProcessor1.Execute;

    //end;
    //-------------------------- Завершение транзакции
         form1.Zconnection2.Commit;
         form1.write_log('$$$$$    УСПЕШНО ДОБАВЛЕНА процедура '+trim(nameproc)+'  $$$$$$$$$$$');
    except
         form1.ZConnection2.Rollback;
        form1.write_log('!!!4 Ошибка запроса добавления х.процедуры !!!'+#13+ form1.ZQuery2.SQL.Text);
        //if BlobStreamIZ.Size>0 then BlobStreamIZ.Free;
        form1.ZQuery2.Close;
        form1.ZQuery1.Close;
        form1.Zconnection1.disconnect;
        form1.Zconnection2.disconnect;
        //
        Result:=1;
        exit;
     end;
  // Завершаем ПРОЦЕДУРУ
  form1.ZQuery2.Close;
  form1.ZQuery1.Close;
  form1.ZConnection1.Disconnect;
  form1.ZConnection2.Disconnect;
  Result:=0;

      form1.ZSQLMonitor1.Active:=false;
   //form1.ZQuery2.SQL.add(':blob'+inttostr(k));
   //BlobStreamIZ := form1.ZQuery3.CreateBlobStream(form1.ZQuery3.fields[k], bmRead);
   //form1.ZQuery2.ParamByName('blob'+inttostr(k)).LoadFromStream(BlobStreamIZ,form1.ZQuery3.Fields[k].DataType);
   //flag_blob:=true;

 // select z.zag||' '||z.src as src from
 //(SELECT 'CREATE OR REPLACE '||p.proname||'('||pg_catalog.pg_get_function_arguments(p.oid)||')'||' RETURNS '||' '||pg_catalog.pg_get_function_result(p.oid)||' AS $BODY$ ' as zag,
 //(SELECT  a.prosrc||' '||'$BODY$ LANGUAGE plpgsql VOLATILE  COST 100;'  FROM  pg_catalog.pg_proc a,pg_catalog.pg_namespace b WHERE  a.proname=p.proname and b.nspowner=a.proowner and b.nspname='public'
 //) as src
 //FROM pg_catalog.pg_proc p
 //     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
 //WHERE p.proname ~ '^(gettarif)$'
 //      AND pg_catalog.pg_function_is_visible(p.oid)) z;

end;



end.

