unit sync_sprav;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils,forms,
  {$IFDEF LINUX}
  unix,
  {$ENDIF}
  dialogs,
  //ZConnection,
  ZDataset,
  //strutils,
  db;

// Забираем список синхронизируемых таблиц на центральном сервере
// и максимальную дату в таблице и все это кладем в массив
function get_table_sync():boolean;

// Делаем выборки и копируем если нужно данные с центрального сервера на локальный
function copy_data_sync():boolean;


///==================== OLD ========================//
// Синхронизация данных
 function Sync_data():byte;  // 0 - успех
// Расчет локального списка расписаний для реального сервера
 function Sync_local_real():byte;
 // Расчет локального списка расписаний для виртуалок
 function Sync_local_virt():byte;
// Процедура сопоставления типов PostgreSQL и ZeosDBO
function get_type_value(myquery:TZQuery;field_n:integer):string;



implementation
uses
  main,platproc;


// Процедура сопоставления типов PostgreSQL и ZeosDBO
function get_type_value(myquery:TZQuery;field_n:integer):string;
 var
   my_type:TField;
begin
  // если NULL
  if myquery.Fields[field_n].IsNull then
    begin
      Result:='NULL';
      exit;
    end;

  // Разбираем по типам поля
  case myquery.Fields[field_n].DataType of
         // Numeric
         ftSmallint,
         ftInteger,
         ftWord,
         ftFloat,
         ftCurrency,
         ftBytes,
         ftVarBytes,
         ftAutoInc,
         ftLargeint,
         ftGuid: Result:=myquery.Fields[field_n].text;

         // Char
         ftString,
         ftDate,
         ftTime,
         ftDateTime,
         ftBlob,
         ftMemo,
         ftGraphic,
         ftFmtMemo,
         ftTypedBinary,
         ftFixedChar,
         ftWideString,
         ftVariant,
         ftTimeStamp,
         ftFixedWideChar,
         ftWideMemo: Result:=quotedstr(myquery.Fields[field_n].text);

         // Boolean
         ftBoolean:Result:=quotedstr(myquery.Fields[field_n].text);

  else
         Result:=quotedstr(myquery.Fields[field_n].text);
  end;


end;



// Синхронизация данных
function Sync_data():byte;
const maxt = 50;
      interval = '10 day';
 var
   n,m,k,b,j,newlines:integer;
   //kol_zap:integer=0;
   kol_field:integer=0;
   tek_table:string;
   tekfield:string;
   stmp:string='';
   maxcreate:string;
   tocka:byte;
   BlobStreamIZ: TStream;
   //BlobStreamW: TStream;
   //flag_blob:boolean;
   newData: boolean;

    Fieldtypenames : Array [TFieldType] of String[15] =
    (
      'Unknown',
      'String',
      'Smallint',
      'Integer',
      'Word',
      'Boolean',
      'Float',
      'Currency',
      'BCD',
      'Date',
      'Time',
      'DateTime',
      'Bytes',
      'VarBytes',
      'AutoInc',
      'Blob',
      'Memo',
      'Graphic',
      'FmtMemo',
      'ParadoxOle',
      'DBaseOle',
      'TypedBinary',
      'Cursor',
      'FixedChar',
      'WideString',
      'Largeint',
      'ADT',
      'Array',
      'Reference',
      'DataSet',
      'OraBlob',
      'OraClob',
      'Variant',
      'Interface',
      'IDispatch',
      'Guid',
      'TimeStamp',
      'FMTBcd',
      'FixedWideChar',
      'WideMemo'
    );
begin
  Result:=1;
  newData := false;
  form1.write_log('=============================sync_data========================================');
  // ================Проверяем доступность серверов======================
  // Подключаемся к локальному серверу
        If not(Connect2(form1.Zconnection2, 2)) then
           begin
             form1.write_log('!!!67 Локальный сервер ['+ConnectINI[14]+'] - НЕТ СОЕДИНЕНИЯ');
             //application.ProcessMessages;
             //form1.ZConnection1.Disconnect;
             exit;
           end;

  //***********************|||| Очистка таблицы av_shedule_tarif. НАЧАЛО |||');
  If 1<>1 then
       begin
         form1.write_log('|||| Очистка таблицы av_shedule_tarif. НАЧАЛО |||');
             form1.ZQuery2.SQL.Clear;
             //form1.ZQuery2.SQL.Add('select upd_duplicate(''tarif'');');
             //form1.ZQuery2.SQL.Add('fetch all in tarif;');
              form1.ZQuery2.SQL.Add('DELETE from av_shedule WHERE (createdate,id) in ');
              form1.ZQuery2.SQL.Add('(SELECT createdate,id FROM av_shedule  where del=0 ');
              form1.ZQuery2.SQL.Add('EXCEPT ');
              form1.ZQuery2.SQL.Add('SELECT max(createdate),id FROM av_shedule  where del=0 ');
              form1.ZQuery2.SQL.Add('  group by id) RETURNING id; ');
         try
         //If tek_table='av_shedule_atp' then
        //form1.write_log(form1.ZQuery1.SQL.Text);//$
          form1.ZQuery2.open;
         except
          form1.write_log('- Таблица '+tek_table+' недоступна на локальном сервере !');
          form1.write_log('!!!18 ОСТАНОВКА   ПРОДОЛЖЕНИЕ СИНХРОНИЗАЦИИ ДАННЫХ НЕВОЗМОЖНО ');
          //application.ProcessMessages;
          //form1.ZConnection1.Disconnect;
          form1.ZConnection2.Disconnect;
          exit;
         end;
          If form1.ZQuery2.RecordCount>0 then
           form1.write_log('xxx Таблица av_shedule_tarif исправлено '+inttostr(form1.ZQuery2.RecordCount)+' записей тарифа ')
           else
             form1.write_log('-- - Таблица av_shedule_tarif не дубликатов записей тарифа с del=0 ');

          //form1.ZConnection1.Disconnect;
          form1.ZConnection2.Disconnect;
         exit;
     end; //#
       //***********************|||| Очистка таблицы av_shedule_tarif. КОНЕЦ |||');


       // ЗАБИРАЕМ СПИСОК ОБНОВЛЯЕМЫХ ТАБЛИЦ С ЦЕНТРАЛЬНОГО СЕРВЕРА
  form1.ZQuery1.SQL.Clear;
  form1.ZQuery1.SQL.Add('SELECT name_table FROM av_sync_table;');
    try
     form1.ZQuery1.open;
     if form1.ZQuery1.RecordCount=0 then
       begin
         form1.ZQuery1.Close;
         //form1.Zconnection1.disconnect;
         form1.Zconnection2.disconnect;
         form1.write_log('!!!19 Не найдено таблиц для синхронизации данных !');
         //application.ProcessMessages;
         //form1.ZConnection1.Disconnect;
         Result:=0;
         exit;
       end;
     except
        form1.ZQuery1.Close;
        //form1.Zconnection1.disconnect;
        form1.Zconnection2.disconnect;
        form1.write_log('- Невозможно получить список синхронизируемых справочников - НЕТ СОЕДИНЕНИЯ С ЦЕНТРАЛЬНЫМ СЕРВЕРОМ');
        form1.write_log('!!!20 ОСТАНОВКА    ПРОДОЛЖЕНИЕ НЕВОЗМОЖНО ');
        //application.ProcessMessages;
        exit;
    end;


   form1.write_log('- Успешно получен список синхронизируемых справочников.');
   //form1.write_log('- Выполняем синхронизацию по списку:');

   If not form1.CheckBox7.Checked
       //and 1<>1 //#
       then
     begin
        result:=1;
   //application.ProcessMessages;
   // ===================================== ЦИКЛ СИНХРОНИЗАЦИИ ЛОКАЛЬНЫХ СПРАВОЧНИКОВ ПО СПИСКУ
               //======================Открываем транзакцию
              try
                 If not form1.Zconnection2.InTransaction then
                    begin
                      form1.Zconnection2.StartTransaction;
                    end
                 else
                   begin
                     form1.write_log('!!!21 ОСТАНОВКА   Незавершенная транзакция ');
                     //application.ProcessMessages;
                     form1.ZConnection2.Rollback;
                     exit;
                   end;

   //ИДЕМ ПО СПИСКУ ТАБЛИЦ
   for n:=0 to form1.ZQuery1.RecordCount-1 do
      begin
         //-------------------------------- На ЛС определяем для текущей таблицы максимальный timestamp
        // Очищаем прямые таблицы с группировкой данных по createdate

         tek_table:=trim(form1.ZQuery1.FieldByName('name_table').AsString);
         //If tek_table<>'av_users' then //#
          //begin
            //form1.ZQuery1.Next;
            //continue;
          //end; //#

       //form1.write_log('- Обновляются данные таблицы '+tek_table);
         application.ProcessMessages;

         //***********!! УДАЛЯЕМ ВСЕ ЗАПИСИ ИЗ ТАБЛИЦ за последние interva дней !! ***********************
         If 1<>1 then //#
         //If pos('av_shedule', tek_table)=0 then
        begin
             form1.ZQuery2.SQL.Clear;
             form1.ZQuery2.SQL.Add('delete FROM '+tek_table+' where createdate>(now()-interval '+quotedstr(interval)+') returning *;');
         try
            //If tek_table='av_shedule_atp' then
             //showmessage(form1.ZQuery2.SQL.Text);//$
          form1.ZQuery2.open;
         except
          form1.write_log('- Таблица '+tek_table+' недоступна на локальном сервере !');
          form1.write_log('!!!22 ОСТАНОВКА   ПРОДОЛЖЕНИЕ СИНХРОНИЗАЦИИ ДАННЫХ НЕВОЗМОЖНО ');
          //application.ProcessMessages;
          //form1.ZConnection1.Disconnect;
          form1.ZConnection2.Disconnect;
          exit;
          end;
             If form1.ZQuery2.RecordCount>0 then
             form1.write_log('xxx Таблица '+tek_table+' очищена за '+interval+' дней. Удалено '+inttostr(form1.ZQuery2.RecordCount)+' записей.')
           else
             form1.write_log('- - - Таблица ' +tek_table+' удалено 0 записей за '+interval+' дней. ');


         form1.ZQuery1.Next;
         continue;
         end; //#
           //***********!! УДАЛЯЕМ конец

        /// берем последний createdate из локальной таблицы
           begin
             form1.ZQuery2.SQL.Clear;
             form1.ZQuery2.SQL.Add('SELECT distinct(to_char(coalesce(max(createdate),''01-01-1970''),'+quotedstr('dd.mm.yyyy hh24:mi:ss.us')+')) as max1 FROM '+tek_table+';');
           end;
         try
            //If tek_table='av_users' then
             //showmessage(form1.ZQuery2.SQL.Text);//$
          form1.ZQuery2.open;
         except
          form1.write_log('- Таблица '+tek_table+' недоступна на локальном сервере !');
          form1.write_log('!!!23 ОСТАНОВКА   ПРОДОЛЖЕНИЕ СИНХРОНИЗАЦИИ ДАННЫХ НЕВОЗМОЖНО ');
          //application.ProcessMessages;
          //form1.ZConnection1.Disconnect;
          //form1.ZConnection2.Disconnect;
          continue;
         end;
        if form1.ZQuery2.RecordCount>0 then
           maxcreate:=trim(form1.ZQuery2.FieldByName('max1').asString);

        if maxcreate=''  then maxcreate:= '01-01-1970';


        //----------------------------------- Делаем выборку с центрального сервера
        form1.ZQuery3.Connection:=form1.ZConnection1;
        form1.ZQuery3.SQL.Clear;
        form1.ZQuery3.SQL.Add('SELECT *,to_char(createdate,'+quotedstr('dd.mm.yyyy hh24:mi:ss.us')+') as createdatems FROM '+tek_table);
        //stmp:='';
        //stmp:= maxcreate;
         if maxcreate<>''  then
                 form1.ZQuery3.SQL.Add(' where createdate>'+quotedstr(maxcreate)+';');

         try
          //If tek_table='av_users' then
          //    form1.write_log(form1.ZQuery3.SQL.Text);//$
           form1.ZQuery3.open;
         except
          //showmessage(form1.ZQuery3.SQL.Text);
          form1.write_log('- Таблица '+tek_table+' недоступна на локальном сервере !');
          form1.write_log('!!!24 ОСТАНОВКА   ПРОДОЛЖЕНИЕ СИНХРОНИЗАЦИИ ДАННЫХ НЕВОЗМОЖНО ');
          //form1.ZConnection1.Disconnect;
          form1.ZConnection2.Disconnect;
          exit;
         end;
          if form1.ZQuery3.RecordCount=0 then
              begin
               If n>0 then form1.Memo1.Lines.Delete(form1.Memo1.Lines.Count-1);
               //form1.write_log('Таблица '+inttostr(n+1)+' из '+inttostr(form1.ZQuery1.RecordCount)+' | '+tek_table+' - обновление не требуется !');
                form1.ZQuery1.Next;
                continue;
              end;

         // --------------------------------- Обновляем данные из ЦС в ЛС
         //идем по новым записям для таблицы
         j:=0;
         if newlines>0 then newData :=true;
         newlines:=0;
         if form1.ZQuery3.RecordCount>0 then
            begin
               //kol_zap:=0;
                 for m:=1 to form1.ZQuery3.RecordCount do
                   begin
                      inc(j);
                     //flag_blob:=false;
                     If m>1 then form1.Memo1.Lines.Delete(form1.Memo1.Lines.Count-1);
                     form1.Memo1.Lines.add('- Таблица №'+inttostr(n+1)+' из '+inttostr(form1.ZQuery1.RecordCount)+' | '+tek_table+' - выполнено '+inttostr(m)+' из '+inttostr(form1.ZQuery3.RecordCount));
                     application.ProcessMessages;
                     // Вставляем данные из ЦС ZQuery3 в ЛС ZQuery2

                     //****************** ПИШЕМ ПАРТИЯМИ ПО maxt-СТРОЧЕК
                   If j=1 then
                     begin
                      //формируем заголовок с именами столбцов
                     form1.ZQuery2.SQL.Clear;
                     form1.ZQuery2.SQL.add('insert into '+tek_table+' (');
                      kol_field:=0;
                      kol_field:=form1.ZQuery3.FieldCount-2;
                       stmp:='';
                     for k:=0 to kol_field do
                      begin
                          stmp:=stmp+form1.ZQuery3.Fields[k].FieldName;
                         if k<(kol_field) then stmp:=stmp+',';
                      end;
                      form1.ZQuery2.SQL.add(stmp);
                     form1.ZQuery2.SQL.add(') VALUES (');
                     end;

                     If (j>1) and (j<=maxt) and (j<=(form1.ZQuery3.RecordCount))
                         then form1.ZQuery2.SQL.add('), (');
                   //формируем данные
                     stmp:='';
                     for k:=0 to kol_field+1 do
                      begin

                    //Если BLOB or MEMO
                   if k<=(kol_field) then
                     If (form1.ZQuery3.Fields[k].DataType=ftBlob) or (form1.ZQuery3.Fields[k].DataType=ftMemo)
                         then
                     begin
                         //предварительно кладем в запрос строку с данными
                         If stmp<>'' then
                         begin
                            form1.ZQuery2.SQL.add(stmp);
                            stmp:='';
                         end;
                          // перекидываем через Stream
                            // BlobStreamIZ c ЦС
                            // Из ЦС
                            form1.ZQuery2.SQL.add('&blobs'+inttostr(m)+inttostr(k));
                            try
                            BlobStreamIZ := form1.ZQuery3.CreateBlobStream(form1.ZQuery3.fields[k], bmRead);
                            form1.ZQuery2.ParamByName('blobs'+inttostr(m)+inttostr(k)).LoadFromStream(BlobStreamIZ,form1.ZQuery3.Fields[k].DataType);
          //if (tek_table='av_spr_ats') and (form1.ZQuery3.Fields[k].FieldName='foto') then showmessage(form1.ZQuery2.sql.Text);//$
                            //flag_blob:=true;
                            finally
                              BlobStreamIZ.Free;
                            end;
                       end;

                       // Если createdate
                       if (trim(form1.ZQuery3.Fields[k].FieldName)='createdate') then
                            begin
                              //form1.ZQuery2.SQL.add(IFTHEN(
                              If form1.ZQuery3.FieldByName('createdatems').asString=''
                                  then stmp:=stmp+'null'
                                  else stmp:=stmp+ quotedstr(form1.ZQuery3.FieldByName('createdatems').AsString);
                              //kol_field:=kol_field-1;
                            end;

                       // Если не createdate  и не createdatems и не ftblob и не ftmemo (memo,text,varchar,bytea)
                        if not((trim(form1.ZQuery3.Fields[k].FieldName)='createdatems')) and
                           not((trim(form1.ZQuery3.Fields[k].FieldName)='createdate')) and
                           not(form1.ZQuery3.Fields[k].DataType=ftMemo) and
                           not(form1.ZQuery3.Fields[k].DataType=ftBlob) then
                            begin
                               If trim(form1.ZQuery3.Fields[k].Text)='' then
                                 If form1.ZQuery3.Fields[k].IsNull then stmp:=stmp+ 'null' else stmp:=stmp+ quotedstr('')
                                 else stmp:= stmp+ quotedstr(form1.ZQuery3.Fields[k].text);
                              //form1.ZQuery2.SQL.add(IFTHEN(trim(form1.ZQuery3.Fields[k].Text)='',ifthen(form1.ZQuery3.Fields[k].IsNull,'null',quotedstr('')),quotedstr(form1.ZQuery3.Fields[k].text)));
                              //showmessage(tek_table+#13+form1.ZQuery3.Fields[k].FieldName);
                            end;


                      if (k<(kol_field)) then stmp:= stmp+ ',';

             //if (tek_table='av_spr_uslugi') and (form1.ZQuery3.Fields[k].FieldName='swed') then
             //        begin
             //           showmessage(form1.ZQuery2.sql.Text);
             //          //showmessage(Fieldtypenames[form1.ZQuery3.Fields[k].DataType]);
             //        end;
                    end;

                  If stmp<>'' then
                     form1.ZQuery2.SQL.add(stmp);
                     //form1.ZQuery2.SQL.add(',')
            //если счетчик или конец таблицы, формируем конец запроса
             If (j=maxt) or (m=form1.ZQuery3.RecordCount) then
              begin
                newlines:=newlines + j;
               form1.ZQuery2.SQL.add(');');
               if not(form1.ZQuery2.SQL.text='') then
                   begin
                     //showmessage(form1.ZQuery2.SQL.Text);//$
                    //form1.write_log(form1.ZQuery2.SQL.Text);
                     try
                      //if tek_table='av_users' then
                         //form1.write_log(form1.ZQuery2.SQL.Text); //$
                       //showmessage(form1.ZQuery2.SQL.Text); //$
                         form1.ZQuery2.ExecSQL;
                      //kol_zap:=kol_zap+1;
                     except
                      form1.write_log('!!!25 ОШИБКА SQL запроса '+#13+form1.ZQuery2.SQL.Text);
                      form1.ZQuery2.Close;
                      break;
                    //showmessage(form1.ZQuery2.SQL.Text);
                     end;
                   end;

                 j:=0;
                end;
                   //if flag_blob=true then
                   //   begin
                   //    if BlobStreamIZ.Size>0 then BlobStreamIZ.Free;
                   //   end;
                form1.ZQuery3.Next;
                 //If n=5 then break;//$
             end;
                //доп проверка
             If (j>0) then
                 begin
                   Form1.write_log('---!!Warning!!--- '+inttostr(j)+' записей НЕДОПИСАНО в таблицу! ');
                 end;


             form1.write_log('Таблица №'+inttostr(n+1)+' из '+inttostr(form1.ZQuery1.RecordCount)+' | '+tek_table+' - записей добавлено: '+inttostr(newlines));
        end;

     form1.ZQuery1.Next;
   end;
   //-------------------------- Завершение транзакции
       form1.Zconnection2.Commit;
    except
       form1.write_log('!!!26 ОСТАНОВКА   Незавершенная транзакция ');
       //application.ProcessMessages;
       form1.ZConnection2.Rollback;
       exit;
     end;

   if not newData then
     form1.write_log('- Новых данных найдено НЕ БЫЛО...');

     result:=0;
   end; //#

  //
   if (newData or form1.CheckBox7.Checked)
       //and (1<>1) //#
      then
      begin
        result:=1;
       //================= Удаляем дубликаты пришедших записей ===================
                 //======================Открываем транзакцию
              try
                 If not form1.Zconnection2.InTransaction then
                    begin
                      form1.Zconnection2.StartTransaction;
                    end
                 else
                   begin
                     form1.write_log('!!!27 ОСТАНОВКА   Незавершенная транзакция ');
                           //application.ProcessMessages;
                     form1.ZConnection2.Rollback;
                     exit;
                   end;

       form1.write_log(':::::::::::::: УДАЛЕНИЕ НЕАКТУАЛЬНЫХ ЗАПИСЕЙ В ТАБЛИЦАХ :::::::::::::::::::::');
        // ------- Определяем запрос для текущей таблицы на состав полей из ZQuery1-------
         form1.ZQuery3.Connection:=form1.ZConnection1;
         form1.ZQuery3.SQL.Clear;
         form1.ZQuery3.SQL.Add('select name_table as table_name from av_sync_table;');
         try
            form1.ZQuery3.open;
           if form1.ZQuery3.RecordCount=0 then
              begin
                form1.write_log('- Список таблиц недоступен на центральном сервере !');
                form1.write_log('!!!28 ОСТАНОВКА   ПРОДОЛЖЕНИЕ СИНХРОНИЗАЦИИ ДАННЫХ НЕВОЗМОЖНО ');
                      //application.ProcessMessages;
                //form1.ZConnection1.Disconnect;
                form1.ZConnection2.Disconnect;
                exit;
              end;
         except
            form1.write_log('!!!29 ОСТАНОВКА   ПРОДОЛЖЕНИЕ СИНХРОНИЗАЦИИ ДАННЫХ НЕВОЗМОЖНО ');
                  //application.ProcessMessages;
            //form1.ZConnection1.Disconnect;
            form1.ZConnection2.Disconnect;
            exit;
         end;

      for n:=0 to form1.ZQuery3.RecordCount-1 do
       begin
        tek_table:=trim(form1.ZQuery3.FieldByName('table_name').asString);

        //if tek_table='av_spr_kontr_dog' then
        //  begin
        //     form1.write_log('skip av_spr_kontr_dog');
        //       form1.ZQuery3.Next;
        //       continue;
        //  end;
        form1.ZQuery2.SQL.Clear;
         form1.ZQuery2.SQL.Add('select * from information_schema.COLUMNS where table_name='+quotedstr(tek_table)+';');
         try
            form1.ZQuery2.open;
         except
            form1.write_log('- Таблица с данными '+tek_table+' недоступна на локальном сервере !');
            form1.write_log('!!!30 ОСТАНОВКА   ПРОДОЛЖЕНИЕ СИНХРОНИЗАЦИИ ДАННЫХ НЕВОЗМОЖНО ');
                  //application.ProcessMessages;
            //form1.ZConnection1.Disconnect;
            form1.ZConnection2.Disconnect;
            exit;
         end;

         //нет информации о таблице в базе
         if (form1.ZQuery2.RecordCount=0) then
        begin
          form1.write_log('!!!31 НЕ ОБНАРУЖЕНО данных о таблице '+tek_table);
          form1.ZQuery3.Next;
          continue;
          end;
        // Оформляем скрипт сравнения и удаления
        //

      If not form1.CheckBox5.Checked then
        begin  //#
        //  !
        // УДАЛЯЮТСЯ ТОЛЬКО ТЕ ЗАПИСИ, КОТОРЫЕ ДУБЛИРУЮТСЯ ПО ВСЕМ ПОЛЯМ,
        // КРОМЕ CREATEDATE !!
        //  !
        //
          form1.ZQuery4.SQL.Clear;
          form1.ZQuery4.SQL.Add('select sync_dublicate_delete('+quotedstr(tek_table)+');');

               //form1.ZQuery4.SQL.Add('delete from '+trim(tek_table)+' where (');
               //tocka:=0;
               //for b:=0 to form1.ZQuery2.RecordCount-1 do
               //   begin
               //     //if not(trim(form1.ZQuery2.FieldByName('column_name').asString)='id_user')
               //      If not(trim(form1.ZQuery2.FieldByName('column_name').asString)='del')
               //        and not(trim(form1.ZQuery2.FieldByName('column_name').asString)='id_user_first')
               //        and not(trim(form1.ZQuery2.FieldByName('column_name').asString)='createdate_first') then
               //       begin
               //        IF  (not(trim(tek_table)='av_users_arm') and not(trim(form1.ZQuery2.FieldByName('column_name').asString)='id_user'))
               //            or ((trim(tek_table)='av_users_arm') and not(trim(form1.ZQuery2.FieldByName('column_name').asString)='id_usr')) then
               //         begin
               //         if tocka>0 then form1.ZQuery4.SQL.Add(',');
               //         form1.ZQuery4.SQL.Add(trim(form1.ZQuery2.FieldByName('column_name').asString));
               //         tocka:=1;
               //         end;
               //       end;
               //    form1.ZQuery2.Next;
               //   end;
               //form1.ZQuery4.SQL.Add(') not in (SELECT ');
               //form1.ZQuery2.First;
               //tocka:=0;
               //for b:=0 to form1.ZQuery2.RecordCount-1 do
               //   begin
               //     //if     not(trim(form1.ZQuery2.FieldByName('column_name').asString)='id_user')
               //      If not(trim(form1.ZQuery2.FieldByName('column_name').asString)='del')
               //        and not(trim(form1.ZQuery2.FieldByName('column_name').asString)='id_user_first')
               //        and not(trim(form1.ZQuery2.FieldByName('column_name').asString)='createdate_first') then
               //       begin
               //         IF (not(trim(tek_table)='av_users_arm') and not(trim(form1.ZQuery2.FieldByName('column_name').asString)='id_user'))
               //            or ((trim(tek_table)='av_users_arm') and not(trim(form1.ZQuery2.FieldByName('column_name').asString)='id_usr')) then
               //         begin
               //         if tocka>0 then form1.ZQuery4.SQL.Add(',');
               //         if trim(form1.ZQuery2.FieldByName('column_name').asString)='createdate' then
               //           begin
               //             form1.ZQuery4.SQL.Add('max(createdate) as createdate');
               //           end
               //         else
               //           begin
               //            form1.ZQuery4.SQL.Add(trim(form1.ZQuery2.FieldByName('column_name').asString));
               //           end;
               //         tocka:=1;
               //         end;
               //       end;
               //    form1.ZQuery2.Next;
               //   end;
               //form1.ZQuery4.SQL.Add(' FROM '+trim(tek_table)+' GROUP BY ');
               //tocka:=0;
               //form1.ZQuery2.First;
               //for b:=0 to form1.ZQuery2.RecordCount-1 do
               //   begin
               //     //if not(trim(form1.ZQuery2.FieldByName('column_name').asString)='id_user')
               //     IF not(trim(form1.ZQuery2.FieldByName('column_name').asString)='del')
               //        and not(trim(form1.ZQuery2.FieldByName('column_name').asString)='createdate')
               //        and not(trim(form1.ZQuery2.FieldByName('column_name').asString)='id_user_first')
               //        and not(trim(form1.ZQuery2.FieldByName('column_name').asString)='createdate_first') then
               //       begin
               //       IF  (not(trim(tek_table)='av_users_arm') and not(trim(form1.ZQuery2.FieldByName('column_name').asString)='id_user'))
               //           or ((trim(tek_table)='av_users_arm') and not(trim(form1.ZQuery2.FieldByName('column_name').asString)='id_usr')) then
               //         begin
               //         if tocka>0 then form1.ZQuery4.SQL.Add(',');
               //         form1.ZQuery4.SQL.Add(trim(form1.ZQuery2.FieldByName('column_name').asString));
               //         tocka:=1;
               //         end;
               //       end;
               //    form1.ZQuery2.Next;
               //   end;
               //form1.ZQuery4.SQL.Add(' ) RETURNING *;');

               //showmessage(form1.ZQuery4.SQL.Text);//$
            //if tek_table='av_tarif_predv' then form1.write_log(form1.ZQuery4.SQL.Text);//$
                //form1.ZQuery3.Next;//$
                //continue;//$
           try
            form1.ZQuery4.open;
           except
             form1.write_log('!!!32 ОСТАНОВКА ПРОДОЛЖЕНИЕ СИНХРОНИЗАЦИИ ДАННЫХ НЕВОЗМОЖНО ');
              form1.write_log('!!!33 ОШИБКА ЗАПРОСА'+#13+form1.ZQuery4.SQL.Text);
                  //application.ProcessMessages;
            //form1.ZConnection1.Disconnect;
            form1.ZConnection2.Disconnect;
            exit;
           end;
         If form1.ZQuery4.Active then
             begin
               If form1.ZQuery4.RecordCount>0 then
                begin
                 case  form1.ZQuery4.Fields[0].AsInteger of
                        0:  form1.write_log('*** НЕАКТУАЛЬНЫХ ЗАПИСЕЙ НЕ ОБНАРУЖЕНО для таблицы '+tek_table);
                       -1:  form1.write_log('!!!70 ОШИБКА ЗАПРОСА хранимой процедуры'+#13+form1.ZQuery4.SQL.Text);
                 else  form1.write_log('--- '+(form1.ZQuery4.Fields[0].AsString)+' НЕАКТУАЛЬНЫХ ЗАПИСЕЙ УДАЛЕНО из таблицы '+tek_table);
                 end;
                end
                else
                 form1.write_log('*** НЕАКТУАЛЬНЫХ ЗАПИСЕЙ НЕ ОБНАРУЖЕНО для таблицы '+tek_table);
             end;
        end; //#

         //  !!
        // ИЩЕМ полные дубликаты и по CREATEDATE !!
        //  !!
        If form1.CheckBox5.Checked then
        begin
           form1.write_log(':::::::::::::: УДАЛЕНИЕ ПОЛНЫХ ДУБЛИКАТОВ В ТАБЛИЦАХ (по всем полям):::::::::::::::::::::');
            form1.ZQuery4.SQL.Clear;
            form1.ZQuery4.SQL.Add('DELETE FROM '+(tek_table)+' WHERE createdate>(now()-interval '+quotedstr(interval)+') and ctid NOT IN ');
            form1.ZQuery4.SQL.Add('(SELECT max(ctid) FROM '+(tek_table)+' where createdate>(now()-interval '+quotedstr(interval)+')');
            form1.ZQuery4.SQL.Add('GROUP BY '+(tek_table)+'.*) RETURNING *;');

           //form1.write_log(form1.ZQuery4.SQL.Text);//$
           try
            form1.ZQuery4.open;
            //showmessage(form1.ZQuery4.SQL.Text);
           except
            form1.write_log('!!!34 ОШИБКА ЗАПРОСА'+#13+form1.ZQuery4.SQL.Text);
            //form1.ZConnection1.Disconnect;
            form1.ZConnection2.Disconnect;
            exit;
           end;

           If form1.ZQuery4.RecordCount>0 then
             begin
               //form1.write_log('^ '+form1.ZQuery4.SQL.Text);//$
               form1.write_log('^^^ '+inttostr(form1.ZQuery4.RecordCount)+' ПОЛНЫХ дубликатов УДАЛЕНО из таблицы '+tek_table);
               for b:=1 to form1.ZQuery4.RecordCount do
                  begin
                    stmp:='';
                    for j:=0 to form1.ZQuery4.Fields.Count-1 do
                       begin
                        stmp:= stmp + form1.ZQuery4.Fields[j].AsString +', ';
                       end;
                    form1.write_log('|'+inttostr(b)+'| '+stmp);
                    form1.ZQuery4.Next;
                  end;
             end
           else
             form1.write_log('--- ПОЛНЫХ Дубликатов НЕ ОБНАРУЖЕНО для таблицы '+tek_table);
           end;//#
       //end; //#

          form1.ZQuery3.Next;
       end;

      //=========================================================================
     //-------------------------- Завершение транзакции
       form1.Zconnection2.Commit;
    except
       form1.write_log('!!!35 ОСТАНОВКА   Незавершенная транзакция ');
       form1.ZConnection2.Rollback;
     end;

    result:=0;
  end; //#


  //form1.ZConnection1.Disconnect;
  form1.ZConnection2.Disconnect;
  Result:=0;
end;


//************************************************************************************************************
//******************** Расчет локального списка расписаний РЕАЛЬНОГО сервера *********************************
//************************************************************************************************************
function Sync_local_real():byte;
var
    newrec,n,j:integer;
    stmp:string;
begin
   Result:=1;
    form1.write_log('=============================Sync_local_real====================================');
  // ================Проверяем доступность серверов======================
  form1.write_log('+-+-+ Определение доступности Локального сервера ['+connectini[14]+'] -+-+-');

  // Подключаемся к локальному серверу
  If not(Connect2(form1.Zconnection2, 2)) then
    begin
      form1.write_log('!!!36 Локальный сервер: НЕТ СОЕДИНЕНИЯ ');
      exit;
    end;
  //If 1<>1 then //#
  //begin
//======================Открываем транзакцию
  try
   If not form1.Zconnection2.InTransaction then
      begin
         form1.Zconnection2.StartTransaction;
      end
   else
      begin
         form1.write_log('!!!37 ОСТАНОВКА   Незавершенная транзакция ');
         form1.ZConnection2.Rollback;
         form1.ZConnection2.Disconnect;
         exit;
      end;

 //If 1<>1 then //#
  begin
//********************************** Заполняем данными таблицу av_trip**********************************
  form1.write_log('-*- Заполнение таблицы av_trip локальных отрезков расписаний данными...');
//form1.write_log('+*- создаем данные по рейсам [ТРАНЗИТ ОТПРАВЛЕНИЕ]');
form1.ZQuery2.SQL.Clear;
form1.ZQuery2.SQL.Add('select sync_trip2('+quotedstr('trip')+','+connectini[14]+');');
form1.ZQuery2.SQL.Add('FETCH ALL FROM trip;');
//form1.ZQuery2.SQL.Add('select sync_trip('+connectini[14]+');');
//form1.write_log(form1.ZQuery2.SQL.Text);//$
//showmessage(form1.ZQuery2.SQL.Text);
try
  form1.ZQuery2.open;
except
  form1.write_log(form1.ZQuery2.SQL.Text);
 //showmessage(form1.ZQuery2.SQL.Text);
 form1.write_log('!!!38 av_trip ОШИБКА обновления СПИСКА ЛОКАЛЬНЫХ ОТРЕЗКОВ РАСПИСАНИЙ ');
 form1.ZConnection2.Rollback;
 //form1.ZConnection1.Disconnect;
 form1.ZConnection2.Disconnect;
 exit;
end;
if form1.ZQuery2.RecordCount=0 then
  begin
     form1.write_log('_НЕ обнаружено новых данных в таблице локальных отрезков расписаний');
    //form1.write_log('!!!39 av_trip Ошибка выполения хранимой процедуры !');
    //form1.ZConnection2.Rollback;
    //form1.ZConnection2.Disconnect;
    //exit;
    end
else
  form1.write_log('+++ Таблица av_trip УСПЕШНО обновлена! Добавлено записей: '+inttostr(form1.ZQuery2.RecordCount));

end;//#

//********************************** Заполняем данными таблицу av_trip_atp_ats**********************************
//If 1<>1 then //#
begin
form1.write_log('| Обновление таблицы av_trip_atp_ats [ПЕРЕВОЗЧИК + АТС]');
//проверяем, что есть новые записи
form1.ZQuery2.SQL.Clear;
form1.ZQuery2.SQL.Add('select sync_trip_atp_ats_find_new('+quotedstr('atp1')+','+connectini[14]+');');
form1.ZQuery2.SQL.Add('FETCH ALL FROM atp1;');
         try
           form1.ZQuery2.open;
         except
          form1.write_log(form1.ZQuery2.SQL.Text);
          form1.write_log('!!!40 ОШИБКА ОБНОВЛЕНИЯ таблицы [ПЕРЕВОЗЧИК + АТС] ');
          form1.ZConnection2.Rollback;
          //form1.ZConnection1.Disconnect;
          form1.ZConnection2.Disconnect;
          exit;
         end;

         newrec := form1.ZQuery2.RecordCount;

  if form1.ZQuery2.RecordCount=0 then
  begin
    form1.write_log('_НЕ обнаружено новых данных в таблице [ПЕРЕВОЗЧИК + АТС]');
    form1.ZQuery2.Close;
    //form1.write_log(form1.ZQuery2.SQL.Text);
    //form1.ZConnection2.Rollback;
    //form1.ZConnection2.Disconnect;
    //exit;//$
    end
  else
    begin
      //если есть новые записи
      form1.write_log('*** Для таблицы av-trip-atp-ats обнаружены НОВЫЕ записи в кол-ве: '+inttostr(form1.ZQuery2.RecordCount));
      //if 1<>1 then
      //begin  //#
   for n:=1 to form1.ZQuery2.RecordCount do
      begin
        stmp:='';
         for j:=0 to form1.ZQuery2.Fields.Count-2 do
            begin
              stmp:= stmp+ form1.ZQuery2.Fields[j].AsString + '; ';
            end;
          form1.write_log('new atp-> '+stmp);
         form1.ZQuery2.Next;
      end;
    //if 1<>1 then //#
    begin //#
   //----- проверка кол-ва строк таблицы ------
   form1.ZQuery2.Close;
   form1.ZQuery2.SQL.Clear;
   form1.ZQuery2.SQL.Add('select id_shedule FROM av_trip_atp_ats;');
   try
           form1.ZQuery2.open;
         except
          form1.write_log(form1.ZQuery2.SQL.Text);
          form1.write_log('!!!41 ОШИБКА ОБНОВЛЕНИЯ таблицы ПЕРЕВОЗЧИК + АТС ');
          form1.ZConnection2.Rollback;
          //form1.ZConnection1.Disconnect;
          form1.ZConnection2.Disconnect;
          exit;
         end;
    form1.write_log('!info! в таблице av_trip_atp_ats сейчас '+inttostr(form1.ZQuery2.RecordCount)+ ' записей.');


   //удаляем старые записи
   form1.ZQuery2.Close;
   form1.ZQuery2.SQL.Clear;
form1.ZQuery2.SQL.Add('select sync_trip_atp_ats_del_old('+quotedstr('atp2')+','+connectini[14]+');');
form1.ZQuery2.SQL.Add('FETCH ALL FROM atp2;');
         try
           form1.ZQuery2.open;
           //form1.write_log(form1.ZQuery2.SQL.Text);
         except
          form1.write_log(form1.ZQuery2.SQL.Text);
          form1.write_log('!!!42 ОШИБКА ОБНОВЛЕНИЯ таблицы [ПЕРЕВОЗЧИК + АТС] ');
          form1.ZConnection2.Rollback;
          //form1.ZConnection1.Disconnect;
          form1.ZConnection2.Disconnect;
          exit;
         end;

  if form1.ZQuery2.RecordCount>0 then
  begin
      form1.write_log('- Из таблицы av-trip-atp-ats УДАЛЕНО неактуальных записей в кол-ве: '+inttostr(form1.ZQuery2.RecordCount));
   //for n:=1 to form1.ZQuery2.RecordCount do
   //   begin
   //     stmp:='';
   //      for j:=0 to form1.ZQuery2.Fields.Count-2 do
   //         begin
   //           stmp:= stmp + form1.ZQuery2.Fields[j].AsString + '; ';
   //         end;
   //       form1.write_log('del-> '+stmp);
   //      form1.ZQuery2.Next;
   //   end;
  end
  else
      form1.write_log('_НЕАКТУАЛЬНЫХ записей в таблице не обнаружено ');

    //добавляем НОВЫЕ записи
    form1.ZQuery2.Close;
    form1.ZQuery2.SQL.Clear;
    form1.ZQuery2.SQL.Add('select sync_trip_atp_ats_ins_new('+quotedstr('atp3')+','+connectini[14]+');');
    form1.ZQuery2.SQL.Add('FETCH ALL FROM atp3;');
         try
           form1.ZQuery2.open;
           //form1.write_log(form1.ZQuery2.SQL.Text);
         except
          form1.write_log(form1.ZQuery2.SQL.Text);
          form1.write_log('!!!43 ОШИБКА ОБНОВЛЕНИЯ таблицы [ПЕРЕВОЗЧИК + АТС] ');
          form1.ZConnection2.Rollback;
          //form1.ZConnection1.Disconnect;
          form1.ZConnection2.Disconnect;
          exit;
         end;

    if form1.ZQuery2.RecordCount>0 then
        form1.write_log('+++ В таблицу [ПЕРЕВОЗЧИК + АТС] УСПЕШНО добавлено новых записей в кол-ве: '+inttostr(form1.ZQuery2.RecordCount))
      else
        form1.write_log('- НЕТ информации по новым записям в таблице av-trip-atp-ats !');

   end;//#

    end;
   //----- проверка кол-ва строк таблицы ------
  form1.ZQuery2.Close;
   form1.ZQuery2.SQL.Clear;
   form1.ZQuery2.SQL.Add('select id_shedule FROM av_trip_atp_ats;');
        try
           form1.ZQuery2.open;
         except
          form1.write_log(form1.ZQuery2.SQL.Text);
          form1.write_log('!!!44 ОШИБКА ОБНОВЛЕНИЯ таблицы [ПЕРЕВОЗЧИК + АТС] ');
          form1.ZConnection2.Rollback;
          //form1.ZConnection1.Disconnect;
          form1.ZConnection2.Disconnect;
          exit;
         end;

    form1.write_log(':info: в таблице av_trip_atp_ats сейчас '+inttostr(form1.ZQuery2.RecordCount)+ ' записей.');
   //end;
  //form1.ZConnection2.Rollback;
  //form1.ZConnection2.Disconnect;
  //exit;
end;//#

//********************************** Заполняем данными таблицу av_trip_dog_lic**********************************
form1.write_log('| Обновление таблицы av-trip-dog-lic данных по рейсам [ДОГОВОРА И ЛИЦЕНЗИИ + ПЕРЕВОЗЧИК]');
// Проверка что на виртуальном уже обновлены договора и лицензии
form1.ZQuery2.Close;
form1.ZQuery2.SQL.Clear;
form1.ZQuery2.SQL.Add('select sync_trip_dog_lic('+quotedstr('tdog')+');');
form1.ZQuery2.SQL.Add('FETCH ALL FROM tdog;');

 //form1.write_log(form1.ZQuery2.SQL.Text);//$
try
  form1.ZQuery2.open;
except
 form1.Memo1.lines.Add(form1.ZQuery2.SQL.Text);
 form1.write_log('!!!45 ОСТАНОВКА ОШИБКА ОБНОВЛЕНИЯ [ДОГОВОРА И ЛИЦЕНЗИИ + ПЕРЕВОЗЧИК] ');
 form1.ZConnection2.Rollback;
 //form1.ZConnection1.Disconnect;
 form1.ZConnection2.Disconnect;
 exit;
end;
  newrec := form1.ZQuery2.RecordCount;
   if form1.ZQuery2.RecordCount=0 then
     form1.write_log('_НЕ обнаружено новых данных в таблице [ДОГОВОРА И ЛИЦЕНЗИИ + ПЕРЕВОЗЧИК]')
  else
   form1.write_log('+++ В таблицу av_trip_dog_lic УСПЕШНО добавлено записей: '+inttostr(form1.ZQuery2.RecordCount));

    for n:=1 to form1.ZQuery2.RecordCount do
      begin
        stmp:='';
         for j:=0 to form1.ZQuery2.Fields.Count-2 do
            begin
              stmp:= stmp+ form1.ZQuery2.Fields[j].AsString + '; ';
            end;
          form1.write_log('new dog-> '+stmp);
         form1.ZQuery2.Next;
      end;

    //----- проверка кол-ва строк таблицы ------
    form1.ZQuery2.Close;
   form1.ZQuery2.SQL.Clear;
   form1.ZQuery2.SQL.Add('select id_kontr FROM av_trip_dog_lic;');
        try
           form1.ZQuery2.open;
         except
          form1.write_log(form1.ZQuery2.SQL.Text);
          form1.write_log('!!!46 ОШИБКА ОБНОВЛЕНИЯ таблицы av-trip-dog-lic ');
          form1.ZConnection2.Rollback;
          //form1.ZConnection1.Disconnect;
          form1.ZConnection2.Disconnect;
          exit;
         end;

    form1.write_log(':info: в таблице av_trip_dog_lic сейчас '+inttostr(form1.ZQuery2.RecordCount)+ ' записей.');
    if form1.ZQuery2.RecordCount<=newrec then
    begin
       form1.write_log('!!!47 Ошибка. мало записей в таблице av-trip-dog-lic ');
        form1.ZQuery2.Close;
       form1.ZQuery2.SQL.Clear;
       form1.ZQuery2.SQL.Add('select sync_trip_dog_lic('+quotedstr('tdog3')+');');
       form1.ZQuery2.SQL.Add('FETCH ALL FROM tdog3;');
       //form1.write_log(form1.ZQuery2.SQL.Text);//$
       try
       form1.ZQuery2.open;
       except
       form1.Memo1.lines.Add(form1.ZQuery2.SQL.Text);
       form1.write_log('!!!48 ОСТАНОВКА ОШИБКА ОБНОВЛЕНИЯ av-trip-dog-lic ');
       form1.ZConnection2.Rollback;
       //form1.ZConnection1.Disconnect;
       form1.ZConnection2.Disconnect;
       exit;
       end;
        form1.write_log('+ ПОВТОРНО В таблицу [ДОГОВОРА И ЛИЦЕНЗИИ + ПЕРЕВОЗЧИК] УСПЕШНО добавлено записей: '+inttostr(form1.ZQuery2.RecordCount));
    end;

 //-------------------------- Завершение транзакции
  form1.Zconnection2.Commit;
except
   form1.ZConnection2.Rollback;
   form1.write_log('!!!49 ОСТАНОВКА   Незавершенная транзакция ');
   exit;
end;

  //form1.ZConnection1.Disconnect;
  form1.ZConnection2.Disconnect;
  Result:=0;
end;


//************************************************************************************************************
//******************** Расчет локального списка расписаний ВИРТУАЛОК *********************************
//************************************************************************************************************
function Sync_local_virt():byte;
var
    n,j, newrec:integer;
    stmp:string;
begin
   Result:=1;
   form1.write_log('=============================Sync_local_virt====================================');
  // ================Проверяем доступность серверов======================
   form1.write_log('+-+-+ Определение доступности виртуальных серверов -+-+-');

  // Подключаемся к локальному серверу
  If not(Connect2(form1.Zconnection2, 2)) then
    begin
      form1.write_log('!!!50 Локальный сервер: НЕТ СОЕДИНЕНИЯ ');
      exit;
    end;
  //If 1<>1 then //#
  //begin
//======================Открываем транзакцию
  try
   If not form1.Zconnection2.InTransaction then
      begin
         form1.Zconnection2.StartTransaction;
      end
   else
      begin
         form1.write_log('!!!51 ОСТАНОВКА   Незавершенная транзакция ');
         form1.ZConnection2.Rollback;
         form1.ZConnection2.Disconnect;
         exit;
      end;

 //If 1<>1 then //#
  begin
//********************************** Заполняем данными таблицу av_trip**********************************
  form1.write_log('-*- Заполнение таблицы av_trip данными');
//form1.write_log('+*- создаем данные по рейсам [ТРАНЗИТ ОТПРАВЛЕНИЕ]');
form1.ZQuery2.SQL.Clear;
form1.ZQuery2.SQL.Add('select sync_trip_virtual();');
//form1.ZQuery2.SQL.Add('select sync_trip('+connectini[14]+');');
//form1.write_log(form1.ZQuery2.SQL.Text);//$
//showmessage(form1.ZQuery2.SQL.Text);
try
  form1.ZQuery2.open;
except
  form1.write_log(form1.ZQuery2.SQL.Text);
 //showmessage(form1.ZQuery2.SQL.Text);
  form1.write_log('!!!52 av_trip ОШИБКА обновления СПИСКА ЛОКАЛЬНЫХ ОТРЕЗКОВ РАСПИСАНИЙ ');
 //form1.ZQuery2.Close;
 form1.ZConnection2.Rollback;
 //form1.ZConnection1.Disconnect;
 form1.ZConnection2.Disconnect;
 exit;
end;
if form1.ZQuery2.RecordCount=0 then
  begin
    form1.write_log('!!!53 av_trip Ошибка выполения хранимой процедуры !');
    form1.ZConnection2.Rollback;
    form1.ZConnection2.Disconnect;
    exit;
    end;
if form1.ZQuery2.Fields[0].AsInteger<1000 then
  form1.write_log('+++ Таблица av_trip УСПЕШНО обновлена для '+inttostr(form1.ZQuery2.RecordCount)+' Виртуальных серверов!')
  else
    form1.write_log('!!!54 Ошибка обновления av_trip виртуальных серверов ! Код ошибки: '+form1.ZQuery2.Fields[0].asString);

end;//#

//********************************** Заполняем данными таблицу av_trip_atp_ats**********************************
//If 1<>1 then //#
begin
form1.write_log('- Создание av-trip-atp-ats данные по рейсам [ПЕРЕВОЗЧИК + АТС]');

//удаляем старые записи
 form1.ZQuery2.Close;
  form1.ZQuery2.SQL.Clear;
form1.ZQuery2.SQL.Add('select sync_trip_atp_ats_virtual_del('+quotedstr('atp2')+');');
form1.ZQuery2.SQL.Add('FETCH ALL FROM atp2;');
       try
         form1.ZQuery2.open;
       except
        form1.write_log(form1.ZQuery2.SQL.Text);
        form1.write_log('!!!55 ОШИБКА ОБНОВЛЕНИЯ таблицы ПЕРЕВОЗЧИК + АТС ');
        form1.ZConnection2.Rollback;
        //form1.ZConnection1.Disconnect;
        form1.ZConnection2.Disconnect;
        exit;
       end;

if form1.ZQuery2.RecordCount>0 then
begin
    form1.write_log('- Из таблицы av-trip-atp-ats УДАЛЕНО неактуальных записей в кол-ве: '+inttostr(form1.ZQuery2.RecordCount));
 //for n:=1 to form1.ZQuery2.RecordCount do
 //   begin
 //     stmp:='';
 //      for j:=0 to form1.ZQuery2.Fields.Count-2 do
 //         begin
 //           stmp:= stmp + form1.ZQuery2.Fields[j].AsString + '; ';
 //         end;
 //       form1.write_log('del-> '+stmp);
 //      form1.ZQuery2.Next;
 //   end;
end
else
    form1.write_log('_НЕАКТУАЛЬНЫХ записей в таблице не обнаружено ');


form1.ZQuery2.SQL.Clear;
form1.ZQuery2.SQL.Add('select sync_trip_atp_ats_virtual_ins('+quotedstr('tt')+');');
form1.ZQuery2.SQL.Add('FETCH ALL FROM tt;');
         try
           form1.ZQuery2.open;
         except
          form1.write_log(form1.ZQuery2.SQL.Text);
          form1.write_log('!!!56 ОШИБКА ОБНОВЛЕНИЯ таблицы ПЕРЕВОЗЧИК + АТС ');
          form1.ZConnection2.Rollback;
          //form1.ZConnection1.Disconnect;
          form1.ZConnection2.Disconnect;
          exit;
         end;
  if form1.ZQuery2.RecordCount=0 then
    form1.write_log('_Новых данных не обнаружено в таблице данных [ПЕРЕВОЗЧИК + АТС]')
  else
    form1.write_log('+++ В таблицу av_trip_atp_ats [ПЕРЕВОЗЧИК + АТС] УСПЕШНО добавлено записей: '+inttostr(form1.ZQuery2.RecordCount));

   for n:=1 to form1.ZQuery2.RecordCount do
      begin
        stmp:='';
         for j:=0 to form1.ZQuery2.Fields.Count-2 do
            begin
              stmp:= stmp+ form1.ZQuery2.Fields[j].AsString + '; ';
            end;
          form1.write_log('new atp-> '+stmp);
         form1.ZQuery2.Next;
      end;


   //----- проверка кол-ва строк таблицы ------
   form1.ZQuery2.Close;
   form1.ZQuery2.SQL.Clear;
   form1.ZQuery2.SQL.Add('select id_shedule FROM av_trip_atp_ats;');
        try
           form1.ZQuery2.open;
         except
          form1.write_log(form1.ZQuery2.SQL.Text);
          form1.write_log('!!!57 ОШИБКА ОБНОВЛЕНИЯ таблицы ПЕРЕВОЗЧИК + АТС ');
          form1.ZConnection2.Rollback;
          //form1.ZConnection1.Disconnect;
          form1.ZConnection2.Disconnect;
          exit;
         end;

    form1.write_log(':info: в таблице av-trip-atp-ats сейчас '+inttostr(form1.ZQuery2.RecordCount)+ ' записей.');

  //form1.ZConnection2.Rollback;
  //form1.ZConnection2.Disconnect;
  //exit;
 end;//#

//********************************** Заполняем данными таблицу av_trip_dog_lic**********************************
form1.write_log('- Обновление av-trip-dog-lic данных по рейсам [ДОГОВОР И ЛИЦЕНЗИЯ + ПЕРЕВОЗЧИК]');
// Проверка что на виртуальном уже обновлены договора и лицензии
form1.ZQuery2.SQL.Clear;
form1.ZQuery2.SQL.Add('select sync_trip_dog_lic('+quotedstr('tdog2')+');');
form1.ZQuery2.SQL.Add('FETCH ALL FROM tdog2;');

 //form1.write_log(form1.ZQuery2.SQL.Text);//$
try
  form1.ZQuery2.open;
except
 form1.Memo1.lines.Add(form1.ZQuery2.SQL.Text);
 form1.write_log('!!!58 ОСТАНОВКА ОШИБКА ОБНОВЛЕНИЯ ДОГОВОРОВ И ЛИЦЕНЗИЙ ПЕРЕВОЗЧИКОВ ');
 form1.ZConnection2.Rollback;
 //form1.ZConnection1.Disconnect;
 form1.ZConnection2.Disconnect;
 exit;
end;
    newrec := form1.ZQuery2.RecordCount;
   if form1.ZQuery2.RecordCount=0 then
     form1.write_log('_НЕ обнаружено новых данных в таблице [ДОГОВОРА И ЛИЦЕНЗИИ + ПЕРЕВОЗЧИК]')
  else
   form1.write_log('+++ В таблицу [ДОГОВОРА И ЛИЦЕНЗИИ + ПЕРЕВОЗЧИК] УСПЕШНО добавлено записей: '+inttostr(form1.ZQuery2.RecordCount));

    for n:=1 to form1.ZQuery2.RecordCount do
      begin
        stmp:='';
         for j:=0 to form1.ZQuery2.Fields.Count-2 do
            begin
              stmp:= stmp+ form1.ZQuery2.Fields[j].AsString + '; ';
            end;
          form1.write_log('new dog-> '+stmp);
         form1.ZQuery2.Next;
      end;


    //----- проверка кол-ва строк таблицы ------
   form1.ZQuery2.Close;
   form1.ZQuery2.SQL.Clear;
   form1.ZQuery2.SQL.Add('select id_kontr FROM av_trip_dog_lic;');
        try
           form1.ZQuery2.open;
         except
          form1.write_log(form1.ZQuery2.SQL.Text);
          form1.write_log('!!!59 ОШИБКА ОБНОВЛЕНИЯ таблицы av-trip-dog-lic ');
          form1.ZConnection2.Rollback;
          //form1.ZConnection1.Disconnect;
          form1.ZConnection2.Disconnect;
          exit;
         end;

    form1.write_log(':info: в таблице av-trip-dog_lic сейчас '+inttostr(form1.ZQuery2.RecordCount)+ ' записей.');
    if form1.ZQuery2.RecordCount<=newrec then
    begin
       form1.write_log('!!!60 Ошибка. мало записей в таблице av-trip-dog-lic ');
        form1.ZQuery2.Close;
       form1.ZQuery2.SQL.Clear;
       form1.ZQuery2.SQL.Add('select sync_trip_dog_lic('+quotedstr('tdog3')+');');
       form1.ZQuery2.SQL.Add('FETCH ALL FROM tdog3;');
       //form1.write_log(form1.ZQuery2.SQL.Text);//$
       try
       form1.ZQuery2.open;
       except
       form1.Memo1.lines.Add(form1.ZQuery2.SQL.Text);
       form1.write_log('!!!61 ОСТАНОВКА ОШИБКА ОБНОВЛЕНИЯ av-trip-dog-lic ');
       form1.ZConnection2.Rollback;
       //form1.ZConnection1.Disconnect;
       form1.ZConnection2.Disconnect;
       exit;
       end;
        form1.write_log('+ ПОВТОРНО В таблицу [ДОГОВОРА И ЛИЦЕНЗИИ + ПЕРЕВОЗЧИК] УСПЕШНО добавлено записей: '+inttostr(form1.ZQuery2.RecordCount));
    end;

  //-------------------------- Завершение транзакции
  form1.Zconnection2.Commit;
except
 form1.write_log('!!!62 ОСТАНОВКА   Незавершенная транзакция ');
   form1.ZConnection2.Rollback;
   form1.ZConnection2.Disconnect;
   exit;
end;

  //form1.ZConnection1.Disconnect;
  form1.ZConnection2.Disconnect;
  Result:=0;
end;



// Делаем выборки и копируем если нужно данные с центрального сервера на локальный
function copy_data_sync():boolean;
var
  n,k:integer;
  max_local_date:string='';
begin
  result:=false;
  // массив таблиц
        //mas_table_sync:array of array of string; // Массив таблиц для синхронизации
        // mas_table_sync[n,0] - name table
        // mas_table_sync[n,1] - max createdate

  form1.write_log('=========================================================================');
  form1.write_log('КОПИРОВАНИЕ:  Забираем данные таблиц из ЦС если необходимо для синхронизации...');

  // Идем по списку выбранных серверов
  for k:=0 to form1.StringGrid1.RowCount-1 do
   begin
   //------------------------------------------------------
     // Если центарльный сервер то пропускаем
     if form1.StringGrid1.Cells[0,k]='0' then continue;
     //form1.write_log('СЕРВЕР: '+form1.StringGrid1.Cells[1,k])+'|'+form1.StringGrid1.Cells[2,k]+'|'+form1.StringGrid1.Cells[3,k]+'|'+form1.StringGrid1.Cells[4,k]);

     // Идем по списку таблиц
     for n:=0 to length(mas_table_sync)-1 do
        begin
          //application.ProcessMessages;
          // ================= Определяем максиммальную дату на текущем локальном сервере для текущей обновляемой таблицы -------------
          //showmessage(form1.StringGrid1.Cells[1,k+1]);
          tek_server:=strtoint(form1.StringGrid1.Cells[1,k+1]);
          //set_server('remote');
          max_local_date:='';
          If not(Connect2(form1.Zconnection1, 1)) then
               begin
                 form1.write_log('- Локальный сервер: '+trim(form1.Edit1.Text)+':'+trim(form1.Edit2.Text)+' - НЕТ СОЕДИНЕНИЯ');
                 break;
               end;
          application.ProcessMessages;
          // ЗАБИРАЕМ СПИСОК ОБНОВЛЯЕМЫХ ТАБЛИЦ С ЦЕНТРАЛЬНОГО СЕРВЕРА
          form1.ZQuery1.SQL.Clear;
          form1.ZQuery1.SQL.Add('SELECT to_char(max(createdate),'+quotedstr('dd.mm.yyyy hh24:mi:ss.us')+') as createdate FROM '+trim(mas_table_sync[n,0])+' order by createdate DESC LIMIT 1;');
          try
           form1.ZQuery1.open;
          except
              form1.ZQuery1.Close;
              form1.Zconnection1.disconnect;
              break;
          end;
          if form1.ZQuery1.RecordCount=0 then
              begin
                form1.ZQuery1.SQL.Clear;
                form1.ZQuery1.SQL.Add('SELECT to_char(current_date-10000,'+quotedstr('dd.mm.yyyy hh24:mi:ss.us')+') as createdate;');
                try
                 form1.ZQuery1.open;
                except
                    form1.ZQuery1.Close;
                    form1.Zconnection1.disconnect;
                    break;
                end;
                max_local_date:=form1.ZQuery1.FieldByName('createdate').asString;
              end
          else
              begin
               max_local_date:=form1.ZQuery1.FieldByName('createdate').asString;
              end;
          form1.ZQuery1.Close;
          form1.Zconnection1.disconnect;

          // ================= Делаем выборку в текущей таблице и COPY в '/tmp/sync_[name table sync]'
          // ================= Если дата createdate ЛС <= createdate ЦС
          //set_server('local');
          If not(Connect2(form1.Zconnection1, 1)) then
               begin
                 form1.write_log('- Центральный сервер: '+trim(form1.Edit1.Text)+':'+trim(form1.Edit2.Text)+' - НЕТ СОЕДИНЕНИЯ');
                 break;
               end;
          application.ProcessMessages;
          // КОПИРУЕМ COPY(SELECT) НА ЦС В /tmp/sync_[name table sync]
          form1.ZQuery1.SQL.Clear;
          form1.ZQuery1.SQL.Add('copy (SELECT * FROM '+trim(mas_table_sync[n,0])+' where createdate>'+quotedstr(max_local_date)+') to '+('/tmp/sync_')+trim(mas_table_sync[n,0])+';');
          try
           form1.ZQuery1.open;
          except
              form1.ZQuery1.Close;
              form1.Zconnection1.disconnect;
              break;
          end;
          form1.ZQuery1.Close;
          form1.Zconnection1.disconnect;

          // ================= Копируем файл с центрально сервера на локальный
          // scp /home/me/Desktop/file.txt user@192.168.1.100:/home/remote_user/Desktop/file.txt
           {$IFDEF LINUX}
          fpsystem('scp /tmp/sync_'+trim(mas_table_sync[n,0])+' platforma:19781985@'+trim(form1.StringGrid1.Cells[3,k])+':/tmp/'+trim(mas_table_sync[n,0]));
           {$ENDIF}

          // ================= Заливаем скопированный файл на локальном сервере в временную таблицу tmpsync_[name table sync]




          //set_server('local');
        end;
     //*************************** В ТРАНЗАКЦИИ ЗАЛИВАЕМ ДАННЫЕ tmpsync_[name table sync] в name table sync
     //*************************** И УДАЛЯЕМ ДУБЛИКАТЫ




     // ************************** ЗАКТЫВАЕМ ТРАНЗАКЦИЮ



   //----------------- ПЕРЕХОД К ОБНОВЛЕНИЮ СЛЕДУЮЩЕГО СЕРВЕРА-------------------------------------
   end;











  // Подключаемся к центральному серверу
  If not(Connect2(form1.Zconnection1, 1)) then
    begin
      form1.write_log('- Центральный сервер: '+trim(form1.Edit1.Text)+':'+trim(form1.Edit2.Text)+' - НЕТ СОЕДИНЕНИЯ');
      result:=false;
      exit;
    end;
  application.ProcessMessages;
  // ЗАБИРАЕМ СПИСОК ОБНОВЛЯЕМЫХ ТАБЛИЦ С ЦЕНТРАЛЬНОГО СЕРВЕРА
  form1.ZQuery1.SQL.Clear;
  form1.ZQuery1.SQL.Add('SELECT name_table FROM av_sync_table;');
    try
     form1.ZQuery1.open;
     if form1.ZQuery1.RecordCount=0 then
       begin
         form1.ZQuery1.Close;
         form1.Zconnection1.disconnect;
         form1.write_log('--Обновление данных отключено на ЦС   ОПЕРАЦИЯ ВЫПОЛНЕНА УСПЕШНО ');
         Result:=false;
         exit;
       end;
     except
     form1.ZQuery1.Close;
     form1.Zconnection1.disconnect;
     form1.write_log('- Невозможно получить список синхронизируемых справочников - НЕТ СОЕДИНЕНИЯ С ЦЕНТРАЛЬНЫМ СЕРВЕРОМ');
     form1.write_log('!!!63 ОСТАНОВКА    ПРОДОЛЖЕНИЕ НЕВОЗМОЖНО ');
     Result:=false;
     exit;
    end;
    // Пишем список в массив
    for n:=0 to form1.ZQuery1.RecordCount-1 do
      begin
         application.ProcessMessages;
        SetLength(mas_table_sync,length(mas_table_sync)+1,2);
        mas_table_sync[length(mas_table_sync)-1,0]:=form1.ZQuery1.FieldByName('name_table').asString;
        form1.ZQuery1.Next;
      end;


  // Запрашиваем max createdate для каждой таблицы в списке
  for n:=0 to length(mas_table_sync)-1 do
   begin
     application.ProcessMessages;
    form1.ZQuery1.SQL.Clear;
    form1.ZQuery1.SQL.Add('SELECT to_char(max(createdate),'+quotedstr('dd.mm.yyyy hh24:mi:ss.us')+') as createdate FROM '+mas_table_sync[n,0]+' order by createdate DESC limit 1;');
      try
       form1.ZQuery1.open;
       except
       form1.ZQuery1.Close;
       form1.Zconnection1.disconnect;
       form1.write_log('- Невозможно получить максимальную дату для обновления таблицы - '+mas_table_sync[n,0]);
       form1.write_log('!!!64 ОСТАНОВКА    ПРОДОЛЖЕНИЕ НЕВОЗМОЖНО ');
       Result:=false;
       exit;
      end;
      mas_table_sync[n,1]:=form1.ZQuery1.FieldByName('createdate').asString;
   end;
   form1.write_log('- Успешно получен список синхронизируемых справочников.');
   form1.write_log('');
   form1.write_log('- Выполняем синхронизацию по списку:');
   form1.write_log('');

   form1.ZQuery1.Close;
   form1.Zconnection1.disconnect;
   result:=true;
end;



// Забираем список синхронизируемых таблиц на центральном сервере
// и максимальную дату в таблице и все это кладем в массив
function get_table_sync():boolean;
 var
   n:integer;
begin
   result:=false;
   // Очищаем массив таблиц
         //mas_table_sync:array of array of string; // Массив таблиц для синхронизации
         // mas_table_sync[n,0] - name table
         // mas_table_sync[n,1] - max createdate

   SetLength(mas_table_sync,0,0);
   form1.write_log('=========================================================================');
   form1.write_log('*                           СИНХРОНИЗАЦИЯ СПРАВОЧНИКОВ                  *');
   form1.write_log('=========================================================================');
   form1.write_log('ПОДГОТОВКА:  Забираем список таблиц для синхронизации...');

   // Подключаемся к центральному серверу
   If not(Connect2(form1.Zconnection1, 1)) then
     begin
       form1.write_log('- Центральный сервер: '+trim(form1.Edit1.Text)+':'+trim(form1.Edit2.Text)+' - НЕТ СОЕДИНЕНИЯ');
       result:=false;
       exit;
     end;
   application.ProcessMessages;
   // ЗАБИРАЕМ СПИСОК ОБНОВЛЯЕМЫХ ТАБЛИЦ С ЦЕНТРАЛЬНОГО СЕРВЕРА
   form1.ZQuery1.SQL.Clear;
   form1.ZQuery1.SQL.Add('SELECT name_table FROM av_sync_table;');
     try
      form1.ZQuery1.open;
      if form1.ZQuery1.RecordCount=0 then
        begin
          form1.ZQuery1.Close;
          form1.Zconnection1.disconnect;
          form1.write_log('--Обновление данных отключено на ЦС   ОПЕРАЦИЯ ВЫПОЛНЕНА УСПЕШНО ');
          Result:=false;
          exit;
        end;
      except
      form1.ZQuery1.Close;
      form1.Zconnection1.disconnect;
      form1.write_log('- Невозможно получить список синхронизируемых справочников - НЕТ СОЕДИНЕНИЯ С ЦЕНТРАЛЬНЫМ СЕРВЕРОМ');
      form1.write_log('!!!65 ОСТАНОВКА    ПРОДОЛЖЕНИЕ НЕВОЗМОЖНО ');
      Result:=false;
      exit;
     end;
     // Пишем список в массив
     for n:=0 to form1.ZQuery1.RecordCount-1 do
       begin
          application.ProcessMessages;
         SetLength(mas_table_sync,length(mas_table_sync)+1,2);
         mas_table_sync[length(mas_table_sync)-1,0]:=form1.ZQuery1.FieldByName('name_table').asString;
         form1.ZQuery1.Next;
       end;


   // Запрашиваем max createdate для каждой таблицы в списке
   for n:=0 to length(mas_table_sync)-1 do
    begin
      application.ProcessMessages;
     form1.ZQuery1.SQL.Clear;
     form1.ZQuery1.SQL.Add('SELECT to_char(max(createdate),'+quotedstr('dd.mm.yyyy hh24:mi:ss.us')+') as createdate FROM '+mas_table_sync[n,0]+' order by createdate DESC limit 1;');
       try
        form1.ZQuery1.open;
        except
        form1.ZQuery1.Close;
        form1.Zconnection1.disconnect;
        form1.write_log('- Невозможно получить максимальную дату для обновления таблицы - '+mas_table_sync[n,0]);
        form1.write_log('!!!66 ОСТАНОВКА    ПРОДОЛЖЕНИЕ НЕВОЗМОЖНО ');
        Result:=false;
        exit;
       end;
       mas_table_sync[n,1]:=form1.ZQuery1.FieldByName('createdate').asString;
    end;
    form1.write_log('- Успешно получен список синхронизируемых справочников.');
    form1.write_log('');
    form1.write_log('- Выполняем синхронизацию по списку:');
    form1.write_log('');

    form1.ZQuery1.Close;
    form1.Zconnection1.disconnect;
    result:=true;
 end;



end.

