unit main;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils, LazFileUtils, Forms, Controls, Graphics, Dialogs, StdCtrls,
  ExtCtrls, ComCtrls, Grids, IniPropStorage, Buttons, DateTimePicker,
  ZConnection, ZDataset, ZSqlProcessor, ZSqlMonitor, platproc,
  //lclproc,
  LazUtf8,
  version_info,
  sync_sprav, sync_proc, auth, Types, math;


type

  { TForm1 }

  TForm1 = class(TForm)
    BitBtn1: TBitBtn;
    BitBtn3: TBitBtn;
    Button1: TButton;
    Button10: TButton;
    Button2: TButton;
    Button3: TButton;
    Button4: TButton;
    Button5: TButton;
    Button6: TButton;
    Button7: TButton;
    Button8: TButton;
    Button9: TButton;
    CheckBox1: TCheckBox;
    CheckBox2: TCheckBox;
    CheckBox3: TCheckBox;
    CheckBox4: TCheckBox;
    CheckBox5: TCheckBox;
    CheckBox6: TCheckBox;
    CheckBox7: TCheckBox;
    CheckBox8: TCheckBox;
    clock_timer: TTimer;
    Edit1: TEdit;
    Edit2: TEdit;
    Edit3: TEdit;
    Edit4: TEdit;
    Edit5: TEdit;
    Edit6: TEdit;
    Edit7: TEdit;
    GroupBox1: TGroupBox;
    GroupBox2: TGroupBox;
    GroupBox3: TGroupBox;
    IdleTimer1: TIdleTimer;
    Image1: TImage;
    Image2: TImage;
    IniPropStorage1: TIniPropStorage;
    Label1: TLabel;
    Label10: TLabel;
    Label11: TLabel;
    Label12: TLabel;
    Label13: TLabel;
    Label14: TLabel;
    Label15: TLabel;
    Label16: TLabel;
    Label17: TLabel;
    Label18: TLabel;
    Label19: TLabel;
    Label2: TLabel;
    Label3: TLabel;
    Label4: TLabel;
    Label5: TLabel;
    Label6: TLabel;
    Label7: TLabel;
    Label8: TLabel;
    Label9: TLabel;
    Memo1: TMemo;
    PageControl1: TPageControl;
    Panel1: TPanel;
    ProgressBar1: TProgressBar;
    SaveDialog1: TSaveDialog;
    Splitter1: TSplitter;
    StringGrid1: TStringGrid;
    StringGrid2: TStringGrid;
    StringGrid3: TStringGrid;
    TabSheet1: TTabSheet;
    auto_sync: TTimer;
    TabSheet2: TTabSheet;
    ZConnection1: TZConnection;
    ZConnection2: TZConnection;
    ZQuery1: TZReadOnlyQuery;
    ZQuery2: TZQuery;
    ZQuery3: TZQuery;
    ZQuery4: TZQuery;
    ZSQLMonitor1: TZSQLMonitor;
    ZSQLProcessor1: TZSQLProcessor;
    DateTimePicker1: TDateTimePicker;
    procedure auto_syncTimer(Sender: TObject);
    procedure BitBtn1Click(Sender: TObject);
    procedure BitBtn2Click(Sender: TObject);
    procedure Button10Click(Sender: TObject);
    procedure Button1Click(Sender: TObject);
    procedure Button2Click(Sender: TObject);
    procedure Button3Click(Sender: TObject);
    procedure Button4Click(Sender: TObject);
    procedure Button5Click(Sender: TObject);
    procedure Button6Click(Sender: TObject);
    procedure Button7Click(Sender: TObject);
    procedure Button8Click(Sender: TObject);
    procedure Button9Click(Sender: TObject);
    procedure CheckBox1Change(Sender: TObject);
    procedure CheckBox2Change(Sender: TObject);
    procedure CheckBox3Change(Sender: TObject);
    procedure CheckBox6Change(Sender: TObject);
    procedure CheckBox7Change(Sender: TObject);
    procedure CheckBox8Change(Sender: TObject);
    procedure clock_timerTimer(Sender: TObject);
    procedure FormCreate(Sender: TObject);
    // Создание списка обновляемых серверов
    function create_list_servers(flcheck:boolean):boolean;  // true - успех
    // Создание списка обновляемых процедур
    function create_list_proc(): boolean;  // true - успех
    procedure FormShow(Sender: TObject);
    procedure StringGrid1DrawCell(Sender: TObject; aCol, aRow: Integer;
      aRect: TRect; aState: TGridDrawState);
    procedure StringGrid2DrawCell(Sender: TObject; aCol, aRow: Integer;
      aRect: TRect; aState: TGridDrawState);
    // Обновляем GRID серверов
    procedure Update_grid;
    // Обновляем GRID хранимых процедур
    procedure Update_grid_proc;
    // Запись лога выполнения операций
    procedure write_log(oper:string);
    // Ищем выбранные сервера в массиве
    function select_servers(id:string):boolean;
    // Пишем настройки
    procedure write_settings(flag:byte);
    // Читаем настройки
    procedure read_settings;
    // Запись лога синхронизации в базу данных
    function DataBaseLog(success:integer; idpnt:string):boolean;
    //синхронизация
    procedure StartS(modeAuto:boolean);
    //изменить интерфейс
    procedure change_interface();
    // protection
    procedure make_up();
    // сохранить порядок отмеченных серверов
    procedure save_checked_servers();
    //проверка сервера на блокировку
    function lock_check():boolean;
    //синхронизация
    procedure syncOperation(idpoint:string; flagReal:boolean);
    //проверка необходимости в обновлении сервера
    function syncNeedCheck(idpoint:string; period:string):string;
    function syncFailsTry(idpoint:string):boolean;
    function getvalidip():boolean;
    function logs_check_first(): boolean;  //журнал проверка запись запуска
    function logs_sync_start(idserv: string): string;  //журнал запись старта синхры
    //журнал запись результата
    procedure logs_sync_result(idserv: string; stamp: string; startline: integer);
  private
    { private declarations }
  public
    { public declarations }
  end;

const
   timeout_signal=300; //предудпреждение перед закрытием
   mas_serv_size =10;
   intervalTime = 10; //интервал запрета синхры

var
  Form1: TForm1;
  flagProfile:integer=1;  //профиль - центральный реальный сервер
  Info:string='';
  flclose:boolean=true; //закрывать формы
  timeout_global:integer=0;  //счетчик таймер бездействия (перед окном закрытия форм операций)
  timeout_local:integer=0;
  virt_sync_data:boolean=false;
  flag_real:boolean=false;
  id_user :integer;
  superuser: boolean=true;
  oppgroup: boolean = false;
  oppUsers: string = '5,9,11,14,115,410,70';
  oppServers:string = '381,814';
  serversOmitted: string = '381';
  intervalOpp: integer = 15;
  flagexit:boolean = false;
  flagSync:boolean = false; //флаг работы синхронизации
  flagtimer:boolean =false;  //запустить таймер автообновления
  fllog:boolean = false; //вести журнал

  mas_serv_loc:array of array of string; //массив серверов для обновления
  // mas_serv_loc[n,0] - id_point
  // mas_serv_loc[n,1] - name
  // mas_serv_loc[n,2] - ip
  // mas_serv_loc[n,3] - port
  // mas_serv_loc[n,4] - base
  // mas_serv_loc[n,5] - login
  // mas_serv_loc[n,6] - passwd
  // mas_serv_loc[n,7] - check
  // mas_serv_loc[n,8] - last createdate

  mas_serv_proc:array of array of string; //массив процедур для обновления
  // mas_serv_proc[n,0] - procname
  // mas_serv_proc[n,1] - procparam

  mas_table_sync: array of array of string;

  //========================= Перемнные из INI для списка серверов ==========================
  ini_local:boolean=false;
  ini_select_server:boolean=false;
  mas_server:array of string; //Список выбранных и сохранненных id серверов


implementation

{$R *.lfm}

{ TForm1 }

//журнал запись результата
procedure TForm1.logs_sync_result(idserv: string; stamp: string; startline: integer);
 var
  n:integer;
   //S: TMemoryStream;
begin
  //S:=TMemoryStream.Create;

  with form1 do
      begin
  // Подключаемся к центральному серверу
  If not(Connect2(form1.Zconnection1, 1)) then
     begin
       write_log('[Журнал синхронизации]: Центральный сервер: '+trim(form1.Edit1.Text)+':'+trim(form1.Edit2.Text)+' - НЕТ СОЕДИНЕНИЯ -k11-');
       exit;
     end;

   //запись в базу и получение штампа времени
    ZQuery1.SQL.Clear;
    ZQuery1.SQL.Add('UPDATE av_update_detail set sync_end=current_timestamp, logs=');
    ZQuery1.SQL.Add('E'+quotedstr(Memo1.Lines[startline])+quotedstr('\n'));
    for n:=startline+1 to Memo1.Lines.Count-1 do
       //for n:=1 to 5 do
     begin
      ZQuery1.SQL.Add(quotedstr(Memo1.Lines[n])+quotedstr('\n'));
      end;
    //form1.Memo1.Lines.savetostream(s);
    //s.Seek(0, soFromBeginning);
    //ZQuery1.sql.LoadFromStream(s);
    ZQuery1.SQL.Add(' where sync_start= '+quotedstr(stamp) +';');
     //showmessage(form1.ZQuery1.SQL.Text); //$
    try
     ZQuery1.ExecSQL;
       except
          //s.free;
      form1.ZQuery1.Close;
      form1.Zconnection1.disconnect;
      write_log('[Журнал синхронизации]: !!! ОШИБКА ЗАПРОСА -k-');
      exit;
     end;

     //s.free;
     form1.ZQuery1.Close;
     form1.ZConnection1.Disconnect;

   end;
end;


//журнал запись старта синхры
function TForm1.logs_sync_start(idserv: string): string;  // true - успех
 var
  n:integer;
begin
  result:='';
  with form1 do
      begin
  // Подключаемся к центральному серверу
  If not(Connect2(form1.Zconnection1, 1)) then
     begin
       write_log('[Журнал синхронизации]: Центральный сервер: '+trim(form1.Edit1.Text)+':'+trim(form1.Edit2.Text)+' - НЕТ СОЕДИНЕНИЯ -k11-');
       exit;
     end;
    //======================Открываем транзакцию
      try
        If not Zconnection1.InTransaction then
           Zconnection1.StartTransaction
        else
          begin
                     form1.write_log('!!!12 ОСТАНОВКА   Незавершенная транзакция -k-');
                     //application.ProcessMessages;
                     ZConnection1.Rollback;
                     Zconnection1.disconnect;
                     exit;
            end;
   //запись в базу и получение штампа времени
   form1.ZQuery1.SQL.Clear;
   form1.ZQuery1.SQL.Add('INSERT INTO av_update_detail(id_user,id_server) VALUES ( '+inttostr(id_user)+','+idserv
    +') returning to_char(sync_start, ''YYYY-MM-DD HH24:MI:SS.US'');');
   //showmessage(form1.ZQuery1.SQL.Text); //$
   ZQuery1.open;
   if form1.ZQuery1.RecordCount>0 then
        begin
         result:=ZQuery1.Fields[0].asString;
        end;

   form1.Zconnection1.Commit;
    except
       form1.ZConnection1.Rollback;
        form1.ZQuery1.Close;
      form1.Zconnection1.disconnect;
      write_log('[Журнал синхронизации]: !!! ОШИБКА ЗАПРОСА -k-');
      exit;
     end;
     form1.ZQuery1.Close;
     form1.ZConnection1.Disconnect;
   end;
end;


// Проверить логи
function TForm1.logs_check_first(): boolean;  // true - успех
 var
  n:integer;
begin
  result:=false;
  with form1.ZQuery1, form1.Zconnection1 do
      begin
  // Подключаемся к центральному серверу
  If not(Connect2(form1.Zconnection1, 1)) then
     begin
       write_log('[Журнал синхронизации] '+trim(form1.Edit1.Text)+':'+trim(form1.Edit2.Text)+' - НЕТ СОЕДИНЕНИЯ -k11-');
       exit;
     end;

  //
   form1.ZQuery1.SQL.Clear;
   form1.ZQuery1.SQL.Add('select ipaddr, id_user, id_server, to_char(sync_start, ''DD-MM HH24:MI:SS'') as stamp '
     +',(select btrim(name) from av_spr_point where id=a.id_server and del=0 order by createdate desc limit 1) as pname '
     +',(select btrim(b.name) from av_users b where b.id=a.id_user order by b.del asc, b.createdate desc limit 1) as username '
     +' from av_update_detail a where id_server<>0 order by sync_start desc limit 1;');

   //showmessage(form1.ZQuery1.SQL.Text);
   try
     open;
     except
      form1.ZQuery1.Close;
      form1.Zconnection1.disconnect;
      write_log('[Журнал синхронизации]: !!! ОШИБКА ЗАПРОСА -k-');
      exit;
     end;

      if form1.ZQuery1.RecordCount>0 then
        begin
         write_log('Последнее обновление: '+FieldByName('stamp').asString +'| ['+FieldByName('id_server').asString+'] '+FieldByName('pname').asString
                 +' | ['+FieldByName('id_user').asString+'] '+FieldByName('username').asString
                  + ' | '+ FieldByName('ipaddr').asString );
        end;
   //запись в базу
   if not superuser then
    begin
   form1.ZQuery1.SQL.Clear;
   form1.ZQuery1.SQL.Add('INSERT INTO av_update_detail(id_user) VALUES ( '+inttostr(id_user)+')');
    try
     ExecSQL;
       except
      form1.ZQuery1.Close;
      form1.Zconnection1.disconnect;
      write_log('[Журнал синхронизации]: !!! ОШИБКА ЗАПРОСА -k-');
      exit;
     end;
    end;
     form1.ZQuery1.Close;
     form1.ZConnection1.Disconnect;
     result:=true;

   end;
end;


//определить адрес и возможность синхронизации по таймеру
function TForm1.getValidIp():boolean;
begin
 result:=false;
 // Подключаемся к центральному серверу
 If not(Connect2(form1.Zconnection1, 1)) then
    begin
      write_log('[Проверка сервера на блокировку]: Центральный сервер: '+trim(form1.Edit1.Text)+':'+trim(form1.Edit2.Text)+' - НЕТ СОЕДИНЕНИЯ -k01-');
      exit;
    end;

  form1.ZQuery1.SQL.Clear;
 form1.ZQuery1.SQL.add('select to_char(now(),'+quotedstr('dd.mm.yyyy hh24:mi:ss')+
    ') as date, host(inet_client_addr()), cast(split_part(host(inet_client_addr()),''.'',4) as integer) as mine;');
// form1.memo1.Lines.AddStrings(ZQ.sql);
 try
   form1.ZQuery1.open;
 except
   form1.ZQuery1.Close;
   form1.Zconnection1.disconnect;
   exit;
 end;
 if form1.ZQuery1.RecordCount>0 then
  begin
  //Tek_datetime:=;
  form1.Label19.caption := 'мой адрес IP: ' + form1.ZQuery1.FieldByName('host').asString;
  //можно включать таймер автообновления если ip < 12
  Result := form1.ZQuery1.FieldByName('mine').asInteger < 12;

  end;
   form1.ZQuery1.Close;
   form1.Zconnection1.disconnect;
   exit;

 end;

//проверка сервера на блокировку
function TForm1.lock_check():boolean;
var
 n:integer;
begin
 result:=false;
   write_log('Проверка сервера на блокировку');
 // Подключаемся к центральному серверу
 If not(Connect2(form1.Zconnection1, 1)) then
    begin
      write_log('[Проверка сервера на блокировку]: Центральный сервер: '+trim(form1.Edit1.Text)+':'+trim(form1.Edit2.Text)+' - НЕТ СОЕДИНЕНИЯ -k01-');
      exit;
    end;

  form1.ZQuery1.SQL.Clear;
  form1.ZQuery1.SQL.Add('select sync_lock();');
  //showmessage(form1.ZQuery1.SQL.Text);
  try
     form1.ZQuery1.open;
     if form1.ZQuery1.RecordCount=0 then
       begin
         form1.ZQuery1.Close;
         form1.Zconnection1.disconnect;
         write_log('!!!1 Ошибка работы хранимой процедуры ! -k02-');
         exit;
       end;
    except
     write_log('!!!2 ОШИБКА ЗАПРОСА блокировки сервера!  -k03-');
     write_log(form1.ZQuery1.SQL.Text);
     form1.ZQuery1.Close;
     form1.Zconnection1.disconnect;
     exit;
    end;

    if form1.ZQuery1.Fields[0].asInteger < 2
      then result := true;

    form1.ZQuery1.Close;
    form1.ZConnection1.Disconnect;
end;

procedure TForm1.save_checked_servers();
var
  n: integer;
  fl_checked:boolean;
begin
  fl_checked:= false;
  If form1.StringGrid1.RowCount<2 then exit;
   for n:=1 to form1.StringGrid1.RowCount-1 do
       begin
            if trim(form1.StringGrid1.Cells[0,n])='1' then
            begin
              fl_checked := true;
            break;
            end;
         end;
   If not fl_checked then exit;
   if form1.CheckBox2.Checked then exit;

   setlength(mas_server,0);
    for n:=1 to form1.StringGrid1.RowCount-1 do
       begin
            if trim(form1.StringGrid1.Cells[0,n])='1' then
            begin
               SetLength(mas_server,length(mas_server)+1);
               mas_server[length(mas_server)-1]:=form1.StringGrid1.Cells[1,n];
            end;
         end;
end;

procedure TForm1.make_up();
begin
   form1.Button1.Enabled:=true;
   form1.Button3.Enabled:=true;
   form1.Button4.Enabled:=true;
   form1.Button5.Enabled:=true;
   form1.Button6.Enabled:=true;
   form1.Button7.Enabled:=true;
   form1.BitBtn1.Enabled:=true;
   form1.GroupBox3.Enabled:=true;
   tabsheet1.Enabled:=true;
   form1.Panel1.Visible:=false;
   form1.Button1.Color:=clRed;
   form1.auto_sync.Enabled:=true;
   application.ProcessMessages;
end;

// Запись лога выполнения операций
procedure TForm1.write_log(oper: string);
 var
  log_file:TextFile;
  filename:string;
   MajorNum : String;
   MinorNum : String;
   RevisionNum : String;
   BuildNum : String;
   Info: TVersionInfo;
begin
    Info := TVersionInfo.Create;
   Info.Load(HINSTANCE);
   // grab just the Build Number
   MajorNum := IntToStr(Info.FixedInfo.FileVersion[0]);
   MinorNum := IntToStr(Info.FixedInfo.FileVersion[1]);
   RevisionNum := IntToStr(Info.FixedInfo.FileVersion[2]);
   BuildNum := IntToStr(Info.FixedInfo.FileVersion[3]);
   Info.Free;

  form1.Memo1.lines.Add(FormatDateTime('yyyy-mm-dd h:m:s.z', now())+'| '+oper);
  application.ProcessMessages;

  // Текущее имя файла лога
  filename:=ExtractFilePath(Application.ExeName)+'log/'+trim('sync_'+FormatDateTime('yyyy-mm-dd', now())+'.log');
  // --------Проверяем что уже есть каталог LOG если нет то создаем
  If Not DirectoryExistsUTF8(ExtractFilePath(Application.ExeName)+'log') then
    begin
     CreateDir(ExtractFilePath(Application.ExeName)+'log');
    end;
  {$I-} // отключение контроля ошибок ввода-вывода
   AssignFile(log_file,filename);
    if fileexistsUTF8(filename) then
       Append(log_file) else
         begin
          Rewrite(log_file); // открытие файла для записи
          writeln(log_file,MajorNum+'.'+MinorNum+'.'+RevisionNum+'.'+BuildNum);
          end;
   {$I+} // включение контроля ошибок ввода-вывода
  if IOResult<>0 then // если есть ошибка открытия, то
     Exit;

  writeln(log_file,FormatDateTime('yyyy-mm-dd hh:nn:ss', now())+'| '+oper);
  closefile(log_file);
end;


// Запись лога синхронизации в базу данных
function TForm1.DataBaseLog(success:integer; idpnt:string):boolean;
//var
   //log_file: textfile;
   //n:integer;
begin
  result:= false;
   //kol_attempt:=1;
   //flag_oper:byte=0;
  //form1.write_log('******************************************************************************************');
  //form1.write_log('*********    Запись журнала синхронизации на Центральный сервер:  *************');
  //form1.write_log('******************************************************************************************');

  // ----------------------------Подключаемся к центральному серверу
   if not(form1.ZConnection1.Connected) then
    begin
      If not(Connect2(form1.Zconnection1, 1)) then
        begin
          form1.write_log('- Центральный сервер: '+trim(form1.Edit1.Text)+':'+trim(form1.Edit2.Text)+' - НЕДОСТУПЕН -k04-');
          exit;
        end;
      end;
      // Начинаем транзакцию
     //try
     // If not form1.Zconnection1.InTransaction then
     //        begin
     //          form1.Zconnection1.StartTransaction;
     //        end
     //      else
     //        begin
     //          form1.write_log('!!!1 ОСТАНОВКА   Незавершенная транзакция !!!');
     //          form1.ZConnection1.Rollback;
     //          exit;
     //        end;

          // Делаем результирующую запись на ЦС
         form1.ZQuery1.Close;
          form1.ZQuery1.SQL.Clear;
          form1.ZQuery1.SQL.add('UPDATE av_update_log SET createdate=now(),attempt=attempt+1,sync_result=');
            If success=0 then
           form1.ZQuery1.SQL.add('true')
          else
            form1.ZQuery1.SQL.add('false');
          form1.ZQuery1.SQL.add(' WHERE createdate>(current_date -1) and sync_result=false and id_server='+idpnt);
          form1.ZQuery1.SQL.add(' RETURNING *');
       try
        //showmessage(form1.ZQuery1.SQL.Text);//$
        //form1.write_log(form1.ZQuery1.SQL.Text);//$
        form1.ZQuery1.open;
       except
        form1.write_log('!!!3 ОСТАНОВКА  НЕВОЗМОЖНО СОЗДАТЬ ЗАПИСЬ ЖУРНАЛА СИНХРОНИЗАЦИИ на ЦС !-k05-');
        form1.ZQuery1.Close;
        exit;
       end;
       if form1.ZQuery1.Active then
        if form1.ZQuery1.RecordCount=0 then
         begin
            form1.write_log('Добавление новой записи в журнал синхронизации.-k06-');
            form1.ZQuery1.close;
          form1.ZQuery1.SQL.Clear;
          form1.ZQuery1.SQL.add('INSERT INTO av_update_log(createdate, id_server, attempt, sync_result) VALUES (');
          form1.ZQuery1.SQL.add('now(),'+idpnt+',1,');
          If success=0 then
           form1.ZQuery1.SQL.add('true')
          else
            form1.ZQuery1.SQL.add('false');
          form1.ZQuery1.SQL.add(');');
       try
        //showmessage(form1.ZQuery1.SQL.Text);//$
        //form1.write_log(form1.ZQuery1.SQL.Text);
        form1.ZQuery1.ExecSQL;
       except
        form1.write_log('!!!4 ОСТАНОВКА  НЕВОЗМОЖНО СОЗДАТЬ ЗАПИСЬ ЖУРАНАЛА СИНХРОНИЗАЦИИ на ЦС !-k07-');
        form1.ZQuery1.Close;
        exit;
       end;
       end;
     //-------------------------- Завершение транзакции
    //   form1.Zconnection1.Commit;
    // except
    //  form1.write_log('!!!5 ОСТАНОВКА   Незавершенная транзакция !');
    //  form1.ZConnection1.Rollback;
    //  exit;
    //end;

  //form1.ZQuery1.Close;
  //form1.ZConnection1.Disconnect;
  //form1.write_log('******************************************************************************************');
  form1.write_log('********* Успешно завершена запись журнала на Центральный сервер  *************');
  form1.write_log('*******************************************************************************');
   result:=true;
end;




// Обновляем GRID хранимых процедур
procedure TForm1.Update_grid_proc;
var
  n:integer;
begin
// Заполняем Grid
form1.StringGrid2.RowCount:=1;
for n:=0 to length(mas_serv_proc)-1 do
   begin
     form1.StringGrid2.RowCount:=form1.StringGrid2.RowCount+1;
     form1.StringGrid2.Cells[0,form1.StringGrid2.RowCount-1]:='0';
     form1.StringGrid2.Cells[1,form1.StringGrid2.RowCount-1]:=mas_serv_proc[n,0];
     form1.StringGrid2.Cells[2,form1.StringGrid2.RowCount-1]:=mas_serv_proc[n,1];
   end;
end;



// Создание списка обновляемых процедур
function TForm1.create_list_proc(): boolean;  // true - успех
 var
  n:integer;
begin
  result:=false;
  SetLength(mas_serv_proc,0,0);
  // Подключаемся к центральному серверу
  If not(Connect2(form1.Zconnection1, 1)) then
     begin
       write_log('[Создание списка обновляемых серверов]: Центральный сервер: '+trim(form1.Edit1.Text)+':'+trim(form1.Edit2.Text)+' - НЕТ СОЕДИНЕНИЯ -k08-');
       exit;
     end;

  // ЗАБИРАЕМ СПИСОК ДОСТУПНЫХ ПРОЦЕДУР
   form1.ZQuery1.SQL.Clear;
   form1.ZQuery1.SQL.Add('SELECT  proname,proargnames ');
   form1.ZQuery1.SQL.Add('FROM    pg_catalog.pg_namespace n');
   form1.ZQuery1.SQL.Add('JOIN    pg_catalog.pg_proc p');
   form1.ZQuery1.SQL.Add('ON      pronamespace = n.oid');
   form1.ZQuery1.SQL.Add('WHERE   nspname = '+quotedstr('public')+' and proname not LIKE '+quotedstr('%dblink%')+' order by proname;');
   //--аналогичный запрос
   //SELECT  proname
  //FROM pg_catalog.pg_proc p
  //WHERE pronamespace=2200
  //--p.proname ~ '^aflawless_dohod$'
      //AND pg_catalog.pg_function_is_visible(p.oid)
      //and p.proname !~ '^dblink';
   //showmessage(form1.ZQuery1.SQL.Text);//$
   try
      form1.ZQuery1.open;
      if form1.ZQuery1.RecordCount=0 then
        begin
          form1.ZQuery1.Close;
          form1.Zconnection1.disconnect;
          write_log('[Создание списка обновляемых серверов]: НЕТ ДАННЫХ ПО СЕРВЕРАМ ПРОДАЖ ! -k09-');
          exit;
        end;
     except
      form1.ZQuery1.Close;
      form1.Zconnection1.disconnect;
      write_log('[Создание списка обновляемых серверов]: !!!6 ОШИБКА ЗАПРОСА -k10-');
      write_log(form1.ZQuery1.SQL.Text);
      exit;
     end;

     // Заполняем список доступных процедур
     // mas_serv_proc[n,0] - procname
     // mas_serv_proc[n,1] - procparam
      for n:=0  to form1.ZQuery1.RecordCount-1 do
        begin
          SetLength(mas_serv_proc,length(mas_serv_proc)+1,2);
          mas_serv_proc[length(mas_serv_proc)-1,0]:=form1.ZQuery1.FieldByName('proname').AsString;
          mas_serv_proc[length(mas_serv_proc)-1,1]:=form1.ZQuery1.FieldByName('proargnames').AsString;
          form1.ZQuery1.Next;
        end;
     form1.ZQuery1.Close;
     form1.ZConnection1.Disconnect;
     result:=true;
end;

// Читаем настройки
procedure TForm1.read_settings;
 var
   fileini:string;
   n,j:integer;
   name_serv:string;
   //tek_id:string;
   tmp: string;
begin
  fileini:=ExtractFilePath(Application.ExeName)+'platsync_settings.ini';
  with form1.IniPropStorage1 do
     begin
       inifilename:=fileini;
      IniSection:='AUTO SYNC';
       form1.DateTimePicker1.Time:=strtotime(ReadString('auto_sync_time','00:00'));
      IniSection:='SYNC SERVER'; //указываем секцию
       //mas_server:array of string; //Список выбранных и сохранненных id серверов
       name_serv:= ReadString('list servers','');
      IniSection:='PERMITIONS'; //указываем секцию
        serversOmitted:=  ReadString('servers omitted',serversOmitted);
        oppUsers:=   ReadString('opp users',oppUsers);
        oppServers:= oppServers +','+ ReadString('opp servers omitted',oppServers);
        intervalOpp:=  math.Max(intervalOpp, abs(ReadInteger('opp interval', intervalOpp)));

        if oppServers='' then oppServers:='0';
        if serversOmitted='' then serversOmitted:='0';
        //if intervalOpp='' then intervalOpp:=

        j:=1;
        for n:=1 to length(oppUsers) do
          begin
             if UTF8Copy(oppUsers,n,1)=',' then
                   begin
                       //write_log(inttostr(n)+'- - '+UTF8Copy(oppUsers,j,n-j));
                    try
                     if id_user=strtoint(UTF8Copy(oppUsers,j,n-j)) then
                      begin
                       oppgroup:=true;
                       break;
                      end;
                     except
                      continue;
                      j:=n+1;
                     end;
                      j:=n+1;
                   end;
           if (n=length(oppUsers)) and (n>=j) then
         begin
          try
            if id_user=strtoint(UTF8Copy(oppUsers,j,n-j+1)) then  oppgroup:=true;
          except
             exit;
          end;
         end;
      end;

       setlength(mas_server,0);
           j:=1;
           for n:=1 to length(name_serv) do
              begin
                if UTF8Copy(name_serv,n,1)='|' then
                   begin
                    //tmp:= UTF8Copy(name_serv,j,n-j);
                     SetLength(mas_server,length(mas_server)+1);
                     mas_server[length(mas_server)-1]:=UTF8Copy(name_serv,j,n-j);
                     j:=n+1;
                   end;
              end;
     end;
end;


// Пишем настройки
procedure TForm1.write_settings(flag: byte);
 var
   fileini:string;
   n:integer;
   name_serv:widestring;
begin
  fileini:=ExtractFilePath(Application.ExeName)+'platsync_settings.ini';

  // Пишем новый файл с настройками по умолчанию
  if FileExistsUTF8(fileini)=false then
    begin
         with form1.IniPropStorage1 do
           begin
             inifilename:=fileini;
             IniSection:='AUTO SYNC';
             WriteString('auto_sync_time','01:00');
             IniSection:='SYNC SERVER'; //указываем секцию
             WriteString('list servers','0');
             IniSection:='PERMITIONS'; //указываем секцию
             WriteString('servers omitted',serversOmitted);
             WriteString('opp users',oppUsers);
             WriteString('opp servers omitted',oppServers);
             WriteInteger('opp interval', intervalOpp);
            end;
    end;

  // Пишем значения для автоматического срабатывания
   if flag=1 then
     begin
       with form1.IniPropStorage1 do
           begin
             inifilename:=fileini;
           IniSection:='AUTO SYNC';
             WriteString('auto_sync_time',timetostr(form1.DateTimePicker1.Time));
           IniSection:='SYNC SERVER'; //указываем секцию
              name_serv:='';
              for n:=1 to form1.StringGrid1.RowCount-1 do
                 begin
                   if trim(form1.StringGrid1.Cells[0,n])='1' then name_serv:=name_serv+trim(form1.StringGrid1.Cells[1,n])+'|';
                 end;
              WriteString('list servers',name_serv);
           IniSection:='PERMITIONS'; //указываем секцию
             WriteString('servers omitted',serversOmitted);
             WriteString('opp users',oppUsers);
             WriteString('opp servers omitted',oppServers);
             WriteInteger('opp interval', intervalOpp);
             end;
     end;
   read_settings;
end;

// Ищем выбранные сервера в массиве
function TForm1.select_servers(id:string):boolean;
 var
   n:integer;
begin
    for n:=0 to length(mas_server)-1 do
       begin
         if trim(mas_server[n])=trim(id) then
           begin
             result:=true;
             exit;
           end;
       end;
    result:=false;
end;



// Обновляем GRID серверов
procedure TForm1.Update_grid();
var
  n,nrow:integer;
begin
// Заполняем Grid
  nrow := form1.StringGrid1.Row;

form1.StringGrid1.RowCount:=1;
for n:=0 to length(mas_serv_loc)-1 do
   begin
     form1.StringGrid1.RowCount:=form1.StringGrid1.RowCount+1;
     form1.StringGrid1.Cells[0,form1.StringGrid1.RowCount-1]:=mas_serv_loc[n,7];
     form1.StringGrid1.Cells[1,form1.StringGrid1.RowCount-1]:=mas_serv_loc[n,0];
     form1.StringGrid1.Cells[2,form1.StringGrid1.RowCount-1]:=mas_serv_loc[n,1];
     form1.StringGrid1.Cells[3,form1.StringGrid1.RowCount-1]:=mas_serv_loc[n,2];
     form1.StringGrid1.Cells[4,form1.StringGrid1.RowCount-1]:=mas_serv_loc[n,4];
     form1.StringGrid1.Cells[5,form1.StringGrid1.RowCount-1]:=mas_serv_loc[n,9];
   end;

 if nrow<=form1.StringGrid1.RowCount then
   form1.StringGrid1.Row := nrow;
end;



// Создание списка обновляемых серверов
function TForm1.create_list_servers(flcheck:boolean): boolean;  // true - успех
 var
  n:integer;
begin
  result:=false;
  SetLength(mas_serv_loc,0,0);
  // Подключаемся к центральному серверу
  If not(Connect2(form1.Zconnection1, 1)) then
     begin
       write_log('[Создание списка обновляемых серверов]: Центральный сервер: '+trim(form1.Edit1.Text)+':'+trim(form1.Edit2.Text)+' - НЕТ СОЕДИНЕНИЯ -k11-');
       exit;
     end;

  // ЗАБИРАЕМ СПИСОК ДОСТУПНЫХ СЕРВЕРОВ
   form1.ZQuery1.SQL.Clear;
   if oppgroup then

     form1.ZQuery1.SQL.Add('select sync_list_servers('+quotedstr('tt')+','+quotedstr(oppServers)+');')
     else
     form1.ZQuery1.SQL.Add('select sync_list_servers('+quotedstr('tt')+','+quotedstr(serversOmitted)+');');
   form1.ZQuery1.SQL.Add('FETCH ALL FROM tt');
  // form1.ZQuery1.SQL.Add('select cast(substring(ip2 from 9 for 3) as integer) ipp ');
  // form1.ZQuery1.SQL.Add(',d.point_id as id,d.ip,d.ip2,d.base_name,d.login,d.pwd,d.port,f.name,d.real_virtual ');
  // form1.ZQuery1.SQL.Add(',(select c.createdate from av_update_log c where sync_result=true and c.id_server=f.id order by c.createdate desc limit 1) last_createdate ');
  // form1.ZQuery1.SQL.Add('from av_servers d,av_spr_point f where d.del=0 and f.del=0 and d.active=1 and f.id=d.point_id ');
  // form1.ZQuery1.SQL.Add('and f.id<>381 ');
  //if oppgroup then
  //    form1.ZQuery1.SQL.Add('and d.real_virtual=1 and f.id not in (5,814) ');
  // form1.ZQuery1.SQL.Add('order by d.real_virtual desc, f.name; ');

   //showmessage(form1.ZQuery1.SQL.Text);
   try
      form1.ZQuery1.open;
      if form1.ZQuery1.RecordCount=0 then
        begin
          form1.ZQuery1.Close;
          form1.Zconnection1.disconnect;
          write_log('[Создание списка обновляемых серверов]: НЕТ ДАННЫХ ПО СЕРВЕРАМ ПРОДАЖ ! -k12-');
          exit;
        end;
     except
      form1.ZQuery1.Close;
      form1.Zconnection1.disconnect;
      write_log('[Создание списка обновляемых серверов]: !!!7 ОШИБКА ЗАПРОСА -k13-');
      write_log(form1.ZQuery1.SQL.Text);
      exit;
     end;

     // Заполняем список доступных серверов
     // mas_serv_loc[n,0] - id_point
     // mas_serv_loc[n,1] - name
     // mas_serv_loc[n,2] - ip
     // mas_serv_loc[n,3] - port
     // mas_serv_loc[n,4] - base
     // mas_serv_loc[n,5] - login
     // mas_serv_loc[n,6] - passwd
     // mas_serv_loc[n,7] - check
     for n:=0  to form1.ZQuery1.RecordCount-1 do
        begin
          SetLength(mas_serv_loc,length(mas_serv_loc)+1,mas_serv_size);
          mas_serv_loc[length(mas_serv_loc)-1,0]:=form1.ZQuery1.FieldByName('id').AsString;
          if form1.ZQuery1.FieldByName('real_virtual').asinteger = 0 then
            if form1.CheckBox3.Checked then
               mas_serv_loc[length(mas_serv_loc)-1,1]:= 'Все ВИРТУАЛЬНЫЕ'
              else
                mas_serv_loc[length(mas_serv_loc)-1,1]:= 'All Virtuals'
             else
              mas_serv_loc[length(mas_serv_loc)-1,1]:=form1.ZQuery1.FieldByName('name').AsString;
          mas_serv_loc[length(mas_serv_loc)-1,2]:=ip_del_zero(form1.ZQuery1.FieldByName('ip2').AsString);
          mas_serv_loc[length(mas_serv_loc)-1,3]:=form1.ZQuery1.FieldByName('port').AsString;
          mas_serv_loc[length(mas_serv_loc)-1,4]:=form1.ZQuery1.FieldByName('base_name').AsString;
          mas_serv_loc[length(mas_serv_loc)-1,5]:=form1.ZQuery1.FieldByName('login').AsString;
          mas_serv_loc[length(mas_serv_loc)-1,6]:=form1.ZQuery1.FieldByName('pwd').AsString;
          mas_serv_loc[length(mas_serv_loc)-1,7]:='0';

       //отметка, что сервер выбран
       //if (not flcheck and oppgroup) or not oppgroup then
       if flcheck then
           if select_servers(mas_serv_loc[length(mas_serv_loc)-1,0]) then
              mas_serv_loc[length(mas_serv_loc)-1,7]:='1';

          mas_serv_loc[length(mas_serv_loc)-1,8]:=form1.ZQuery1.FieldByName('real_virtual').AsString;
          mas_serv_loc[length(mas_serv_loc)-1,9]:=form1.ZQuery1.FieldByName('last_createdate').AsString;
          form1.ZQuery1.Next;
        end;
     form1.ZQuery1.Close;
     form1.ZConnection1.Disconnect;
     result:=true;
end;


procedure TForm1.StringGrid1DrawCell(Sender: TObject; aCol, aRow: Integer;
  aRect: TRect; aState: TGridDrawState);
begin
   //if trim(Form1.StringGrid1.Cells[0,aRow])='1' then
   //  begin
   //    Form1.StringGrid1.Canvas.Brush.Color:=clMoneyGreen;
   //  end;
   //
 with Sender as TStringGrid, Canvas do
   begin
         //    Если фокус
         if (gdSelected in aState) then
           begin
            Brush.Color:=clSkyBlue;
            FillRect(aRect);
            pen.Width:=2;
            pen.Color:=clGray;
            MoveTo(aRect.left,aRect.bottom-1);
            LineTo(aRect.right,aRect.Bottom-1);
            MoveTo(aRect.left,aRect.top-1);
            LineTo(aRect.right,aRect.Top);
           end
         else
          begin
           //Раскрашиваем в соответствии с состоянием рейса
          if arow>0 then
            begin
             Brush.Color:=clWhite; //'ОТКРЫТ'
            end;
           end;
            FillRect(aRect);
            pen.Width:=2;
            pen.Color:=clGray;
            MoveTo(aRect.left,aRect.bottom-1);
            LineTo(aRect.right,aRect.Bottom-1);


            // ТЕКСТ
            if (arow>0) and (acol>0) then
              begin
               font.Color:=clBlack;
               if (acol=2) and (trim(mas_serv_loc[arow-1,8])='1') then
                  font.Color:=clBlue;

               font.size:=11;
               //имя отделения
               if acol=2 then
                 font.size:=12;
               //дата время последней синхры
               if acol=5 then
                 font.size:=9;
               font.Style:=[];
               //form1.StringGrid1.Canvas.TextRect(aRect,arow+5,5,form1.StringGrid1.Cells[aCol, aRow]);
               if (acol=1) then
                 DrawCellsAlign(form1.StringGrid1,2,2,form1.StringGrid1.Cells[aCol, aRow],aRect)
               else
                 DrawCellsAlign(form1.StringGrid1,1,2,form1.StringGrid1.Cells[aCol, aRow],aRect);
              end;

            // Чек бокс
            if (arow>0) and (acol=0) then
              begin
               font.Color:=clRed;
               font.size:=22;
               font.Style:=[];
               //form1.StringGrid1.Canvas.TextRect(aRect,arow+5,5,form1.StringGrid1.Cells[aCol, aRow]);
               if trim(Cells[aCol, aRow])='1' then
                begin
                 brush.Color:=clRed;
                 FillRect(aRect);
                 pen.Width:=2;
                 pen.Color:=clGray;
                 MoveTo(aRect.left,aRect.bottom-1);
                 LineTo(aRect.right,aRect.Bottom-1);
                 //textout(arow,acol,'*');
                  //textout(arect.Left+2,arect.Top+2,'*');
                 //DrawCellsAlign(form1.StringGrid1,2,2,form1.StringGrid1.Cells[aCol, aRow],aRect);
                 //DrawCellsAlign(form1.StringGrid1,2,2,'*',aRect);
                end
               else
                begin
                brush.Color:=clWhite;
                FillRect(aRect);
                pen.Width:=2;
                pen.Color:=clGray;
                MoveTo(aRect.left,aRect.bottom-1);
                LineTo(aRect.right,aRect.Bottom-1);
                 //DrawCellsAlign(form1.StringGrid1,2,2,'',aRect);
                 //textout(arow,acol,'');
                end;
            end;

            // Заголовок
            if (arow=0) then
              begin
                Brush.Color:=clDefault;
                FillRect(aRect);
                Font.Color := clBlack;
                font.Size:=10;
                TextOut(aRect.Left+5, aRect.Top+5, Cells[aCol, aRow]);
              end;

   end;

end;

procedure TForm1.StringGrid2DrawCell(Sender: TObject; aCol, aRow: Integer;
  aRect: TRect; aState: TGridDrawState);
begin

 with Sender as TStringGrid, Canvas do
   begin
         //    Если фокус
         if (gdSelected in aState) then
           begin
            Brush.Color:=clSkyBlue;
            FillRect(aRect);
            pen.Width:=2;
            pen.Color:=clGray;
            MoveTo(aRect.left,aRect.bottom-1);
            LineTo(aRect.right,aRect.Bottom-1);
            MoveTo(aRect.left,aRect.top-1);
            LineTo(aRect.right,aRect.Top);
           end
         else
          begin
          if arow>0 then
            begin
             Brush.Color:=clWhite;
            end;
           end;
            FillRect(aRect);
            pen.Width:=2;
            pen.Color:=clGray;
            MoveTo(aRect.left,aRect.bottom-1);
            LineTo(aRect.right,aRect.Bottom-1);


            // ТЕКСТ
            if (arow>0) and (acol>0) then
              begin
               font.Color:=clBlack;
               font.Color:=clBlack;
               font.size:=11;
               font.Style:=[];
               //form1.StringGrid1.Canvas.TextRect(aRect,arow+5,5,form1.StringGrid1.Cells[aCol, aRow]);
               DrawCellsAlign(form1.StringGrid2,1,2,form1.StringGrid2.Cells[aCol, aRow],aRect);
              end;

            // Чек бокс
            if (arow>0) and (acol=0) then
              begin
               font.Color:=clBlue;
               font.size:=24;
               font.Style:=[];
               //form1.StringGrid1.Canvas.TextRect(aRect,arow+5,5,form1.StringGrid1.Cells[aCol, aRow]);
               if trim(Cells[aCol, aRow])='1' then
                begin
                 brush.Color:=clRed;
                 FillRect(aRect);
                 pen.Width:=2;
                 pen.Color:=clGray;
                 MoveTo(aRect.left,aRect.bottom-1);
                 LineTo(aRect.right,aRect.Bottom-1);
                 //textout(arow,acol,'*');
                  //textout(arect.Left+2,arect.Top+2,'*');
                 //DrawCellsAlign(form1.StringGrid1,2,2,form1.StringGrid1.Cells[aCol, aRow],aRect);
                 //DrawCellsAlign(form1.StringGrid1,2,2,'*',aRect);
                end
               else
                begin
                brush.Color:=clWhite;
                FillRect(aRect);
                pen.Width:=2;
                pen.Color:=clGray;
                MoveTo(aRect.left,aRect.bottom-1);
                LineTo(aRect.right,aRect.Bottom-1);
                 //DrawCellsAlign(form1.StringGrid1,2,2,'',aRect);
                 //textout(arow,acol,'');
                end;
            end;

            // Заголовок
            if (arow=0) then
              begin
               font.Color:=clBlack;
               font.size:=10;
               font.Style:=[];
               DrawCellsAlign(form1.StringGrid2,1,2,form1.StringGrid2.Cells[aCol, aRow],aRect);
              end;

   end;


end;



procedure TForm1.clock_timerTimer(Sender: TObject);
var
 n,k:integer;
begin
   // Текущая дата и время
  form1.Label8.Caption:=timetostr(time())+' '+datetostr(now());
  k:=0;
  for n:=1 to form1.StringGrid1.RowCount-1 do
     begin
        if trim(form1.StringGrid1.Cells[0,n])='1' then inc(k);
     end;
  form1.Label11.Caption:=inttostr(k);
end;

procedure TForm1.BitBtn2Click(Sender: TObject);
begin
  flagexit:=true;
  if not flagSync then
  form1.close;
end;

procedure TForm1.Button10Click(Sender: TObject);
var
   n:integer;
begin
   // Снимаем ВСЕ
   for n:=1 to form1.StringGrid2.RowCount-1 do
     begin
       if trim(form1.StringGrid2.Cells[0,n])='1' then form1.StringGrid2.Cells[0,n]:='0' else form1.StringGrid2.Cells[0,n]:='1';
     end;
end;


procedure TForm1.auto_syncTimer(Sender: TObject);
var
   servDone:integer =0;
begin
    //form1.write_log('--------------|!| ТАЙМЕР СРАБОТАЛ |!|---------------');
  if flagSync then
  begin
    form1.write_log('!*!-! Продолжение невозможно! В настоящее время идет синхронизация! -k14-');
    exit;
  end;
  if form1.CheckBox1.Checked=false then exit;
   //showmessage('Погнали');
  //showmessage(timetostr(form1.DateTimePicker1.Time));
  If (strtotime('06:00')>time()) OR (time()>form1.DateTimePicker1.Time) then
  //if copy(timetostr(form1.DateTimePicker1.Time),1,5)<copy(timetostr(Time()),1,5) then
      begin
        try
         servDone := strtoint(form1.syncNeedCheck('0', timetostr(form1.DateTimePicker1.Time)));
        except
         servDone:=0;
        end;
        if servDone>=length(mas_serv_loc) then
          begin
            write_log('Все '+inttostr(servDone)+' локальных сервера уже были обновлены с '+(formatdatetime('dd-mm-yyyy',date())+#32+timetostr(form1.DateTimePicker1.Time)));
            exit;
          end;
        //
        form1.save_checked_servers();
          // актуализируем сервера и их последнее успешной обновление
        if form1.create_list_servers(true) then
         form1.Update_grid();
//   начать синхру
        form1.StartS(true);
         // обновляем список серверов
        if form1.create_list_servers(true) then
         form1.Update_grid();
        end
  else
    write_log(' \-таймер авто обновления-/ ');
end;

procedure TForm1.Button1Click(Sender: TObject);
begin

    form1.save_checked_servers();
   // актуализируем сервера и их последнее успешной обновление
    if form1.create_list_servers(true) then
      form1.Update_grid();
    //exit;
   //процедура обновления
   form1.StartS(false);
    // обновляем список серверов
    if form1.create_list_servers(false) then
      form1.Update_grid();
 end;

//проверка необходимости в обновлении сервера после n неудачных попыток
function TForm1.syncFailsTry(idpoint:string):boolean;
begin
  result:=false;
    // Подключаемся к центральному серверу
 If not(Connect2(form1.Zconnection1, 1)) then
    begin
      form1.write_log('^^ Центральный сервер:  - НЕТ СОЕДИНЕНИЯ -k16-');
      application.ProcessMessages;
      exit;
    end;
  write_log('проверка необходимости в обновлении сервера после n неудачных попыток');

  form1.ZQuery1.SQL.Clear;
  form1.ZQuery1.SQL.Add('select sync_count_fails('+idpoint+');');
  try
     //write_log(form1.ZQuery1.SQL.Text);//$
     form1.ZQuery1.open;
  except
     write_log('!!! ОШИБКА ЗАПРОСА -k17-');
     write_log(form1.ZQuery1.SQL.Text);
     form1.ZQuery1.Close;
     form1.Zconnection1.disconnect;
     exit;
   end;

   if form1.ZQuery1.RecordCount>0 then
       result := form1.ZQuery1.Fields[0].AsBoolean;
    //else
    //   begin
    //     result :=form1.ZQuery1.Fields[0].asInteger;
    //   end;

   form1.ZQuery1.Close;
   form1.Zconnection1.disconnect;
end;


//проверка необходимости в обновлении сервера
function TForm1.syncNeedCheck(idpoint:string; period: string):string;
begin
  result:= '-1';
      write_log('проверка необходимости в обновлении сервера');
    // Подключаемся к центральному серверу
 If not(Connect2(form1.Zconnection1, 1)) then
    begin
      form1.write_log('^^ Центральный сервер:  - НЕТ СОЕДИНЕНИЯ -k18-');
      //application.ProcessMessages;
      exit;
    end;
  //write_log('Проверка сервера на блокировку');

  form1.ZQuery1.SQL.Clear;
  if idpoint='0' then
      form1.ZQuery1.SQL.Add('select sync_need_checkall('+idpoint+','+quotedstr(period)+');')
      else
       form1.ZQuery1.SQL.Add('select sync_need_check('+idpoint+','+quotedstr(period)+');');
  //showmessage(form1.ZQuery1.SQL.Text);
  try
     //write_log(form1.ZQuery1.SQL.Text);//$
     form1.ZQuery1.open;
  except
     write_log('!!! ОШИБКА ЗАПРОСА блокировки сервера! -k19-');
     write_log(form1.ZQuery1.SQL.Text);
     form1.ZQuery1.Close;
     form1.Zconnection1.disconnect;
     exit;
   end;
   if form1.ZQuery1.RecordCount=0 then
       result := '0'
    else
       begin
         if idpoint='0' then
            result :=form1.ZQuery1.Fields[0].asString
            else
             result :=formatDatetime('dd-mm-yyyy hh:nn',form1.ZQuery1.Fields[0].asDatetime);
       end;

   form1.ZQuery1.Close;
   form1.Zconnection1.disconnect;
end;


procedure TForm1.syncOperation(idpoint:string; flagReal:boolean);
var
  resFlag:integer;
begin
  resFlag:= -1;
  form1.write_log('+Определение доступности Центрального сервера');

  // Подключаемся к центральному серверу
 If not(Connect2(form1.Zconnection1, 1)) then
    begin
      form1.write_log('%% Центральный сервер:  - НЕТ СОЕДИНЕНИЯ -k20-');
      //application.ProcessMessages;
      exit;
    end;
  write_log('Проверка сервера на блокировку');

  form1.ZQuery1.SQL.Clear;
  form1.ZQuery1.SQL.Add('select sync_lock('+idpoint+');');
  //showmessage(form1.ZQuery1.SQL.Text);
  try
     form1.ZQuery1.open;
     if form1.ZQuery1.RecordCount=0 then
       begin
         form1.ZQuery1.Close;
         form1.Zconnection1.disconnect;
         write_log('!!! Ошибка работы хранимой процедуры ! -k21-');
         exit;
       end;
    except
     write_log('!!!11 ОШИБКА ЗАПРОСА блокировки сервера! -k22-');
     write_log(form1.ZQuery1.SQL.Text);
     form1.ZQuery1.Close;
     form1.Zconnection1.disconnect;
     exit;
    end;

    if form1.ZQuery1.Fields[0].asInteger = 2  then
      begin
          write_log('! Сервер id='+idpoint+'  заблокирован! Идет синхронизация! Продолжение невозможно! -k23-');
          form1.ZQuery1.Close;
          form1.Zconnection1.disconnect;
          exit;
      end;

    //======================Открываем транзакцию
      try
        If not form1.Zconnection1.InTransaction then
           form1.Zconnection1.StartTransaction
        else
          begin
                     form1.write_log('!!!12 ОСТАНОВКА   Незавершенная транзакция -k24-');
                     //application.ProcessMessages;
                     form1.ZConnection1.Rollback;
                     form1.Zconnection1.disconnect;
                     exit;
            end;

     //блокируем запись
      write_log('Блокируем сервер '+idpoint);
    form1.ZQuery1.SQL.Clear;
    form1.ZQuery1.SQL.Add('SELECT 1 FROM av_update_log WHERE id_server=0  AND createdate> current_date');
    form1.ZQuery1.SQL.Add(' ORDER BY createdate limit 1 FOR UPDATE;');
    form1.ZQuery1.Open;

       //Если синхронизировать данные
       if not form1.CheckBox6.Checked then
             resFlag:=sync_data();
       //Если делать локальные списки
       if not form1.CheckBox8.Checked then
        begin
          if resFlag<1 then
           begin
             if flagReal then
                 resFlag:=sync_sprav.Sync_local_real() //реальные
               else
                 resFlag:=sync_sprav.Sync_local_virt(); //все виртуалки
               end;
          end;

     if resFlag=1 then
       write_log('!!!13 Ошибка выполения обновления для сервера ['+idpoint+']');

       //запись лога в базу
        if (resFlag> -1) then
          If not form1.CheckBox7.Checked
            //and not form1.CheckBox8.Checked
            then
              If not form1.DataBaseLog(resFlag, idpoint) then
                write_log('!!!14 Ошибка создания записи журнала av_update_log !');

     //-------------------------- Завершение транзакции
       form1.Zconnection1.Commit;
    except
       form1.ZConnection1.Rollback;
       form1.ZQuery1.Close;
       form1.ZConnection1.Disconnect;
       form1.write_log('!!!15 ОСТАНОВКА   Незавершенная транзакция ');
             //application.ProcessMessages;
       exit;
     end;
    form1.ZQuery1.Close;
    form1.ZConnection1.Disconnect;
end;


//варианты синхронизации
procedure TForm1.StartS(modeAuto:boolean);
 var
  n:integer;
  gotit:boolean; //флаг отметки сервера на обновление
  doit: boolean;
  timecheck, stamptime: string;
  //stampdone: Tdatetime;
  mlines: integer;//кол-во строк мемо на начало следующего сервера
begin
 mlines := 0;
  flagExit:=false;
  flagSync:=true;
   //form1.auto_sync.Enabled:=false;
   form1.Button1.Color:=clGray;
   form1.Button1.Enabled:=false;
   form1.Button3.Enabled:=false;
   form1.Button4.Enabled:=false;
   form1.Button5.Enabled:=false;
   form1.Button6.Enabled:=false;
   form1.Button7.Enabled:=false;
   form1.BitBtn1.Enabled:=false;
   form1.GroupBox3.Enabled:=false;
   tabsheet1.Enabled:=false;
   form1.Memo1.Clear;
   form1.panel1.Visible:=true;
   application.ProcessMessages;
    //flag_real:=false;
    virt_sync_data:=false;

    if modeAuto then
          form1.write_log('--------------|!| Синхронизация НАЧАЛО (авто режим) |!|---------------')
       else
          form1.write_log('--------------\*/ Синхронизация НАЧАЛО (ручной режим) \*/---------------');
    gotit:=false;//флаг отметки сервера на обновление
   // Цикл по обновлению серверов по порядку в GRID
   form1.Label6.Caption:='';

   //обновление в РУЧНОМ режиме (недоступно ОПП)
   If form1.CheckBox2.Checked then
    begin
      // Если IP и name base совпадают с ЦЕНТРАЛЬНЫМ то пропуск
        if (trim(form1.edit1.Text)=trim(form1.edit3.Text)) and (trim(form1.edit5.Text)=trim(form1.edit6.Text)) then
          begin
         form1.write_log('!!!16 Синхронизируемые базы совпадают между собой !');
         make_up();
         flagSync:=false;
         exit;
         end;

            //flag_real:=false;
            ConnectINI[4]:=trim(form1.edit3.Text);
            ConnectINI[5]:='5432';
            ConnectINI[6]:=trim(form1.edit6.Text);
            ConnectINI[14]:=trim(form1.edit7.Text);

            //ConnectINI[4]:='172.27.1.5';    //&
            //ConnectINI[6]:='platforma_815';//&

            form1.Label6.Caption:=' Обновляется сервер :'+#13+'               '+trim(ConnectINI[4])+':'+trim(ConnectINI[5])+#13+
                                                              '               '+trim(ConnectINI[6])+' ['+trim(ConnectINI[14])+']';
            //form1.write_log(' ');
            form1.write_log('*********** MANUAL SYNC *****************< '+inttostr(n)+' >********************************');
            //form1.write_log('Обновляется сервер:  '+trim(ConnectINI[4])+'  '+trim(ConnectINI[6])+' ['+trim(ConnectINI[14])+']');
            form1.write_log('Обновляется сервер '+form1.edit7.Text+' '+form1.edit3.Text+' ['+ConnectINI[14]+'] ip:'+ConnectINI[4]);

            //определяем реальный или виртуал
            for n:=low(mas_serv_loc) to high(mas_serv_loc) do
              begin
                if ConnectINI[14]=mas_serv_loc[n,0] then
                  begin
                    gotit:=true;
                  if mas_serv_loc[n,8]='1' then
                    syncOperation(ConnectINI[14],true)
                    else
                      syncOperation(ConnectINI[14],false);
                  break;
                  end;
            end;
            if not gotit then
              begin
                write_log('Не найдены настройки указанного сервера '+ConnectINI[14]);
              end;
       // sync_res:=-1;
       // // Синхронизируем справочники и Создаем локальные списки расписаний
       // if not form1.CheckBox6.Checked then
       //    sync_res:=sync_data();
       // //Делаем локальные списки av_trip
       // if not form1.CheckBox8.Checked then
       //       if sync_res=0 then                               //#
       //          sync_res:=sync_sprav.Sync_local_real();    //#
       //
       ////запись лога в базу
       //if sync_res>-1 then
       // if not form1.CheckBox6.Checked and not form1.CheckBox7.Checked
       //     and not form1.CheckBox8.Checked then
       //        form1.DataBaseLog(sync_res);

    end;

   //ЗАПУСК ИЗ РЕЖИМА АВТО
  If form1.CheckBox2.Checked=false then
   begin

   for n:=1 to form1.StringGrid1.RowCount-1 do
      begin

       if mlines > form1.Memo1.Lines.Count then
           mlines := 0;
        if flagExit then
         begin
           write_log('ЭКСТРЕННЫЙ ВЫХОД !-!-!');
          break;
         end;

       stamptime :='';
          //stampDone := strtoDatetime('1970-01-01 01:01');
        // Если IP и name base совпадают с ЦЕНТРАЛЬНЫМ то пропуск
        if (trim(form1.StringGrid1.Cells[3,n])=trim(form1.edit1.Text)) and (trim(form1.StringGrid1.Cells[4,n])=trim(form1.edit5.Text)) then
         begin
         form1.write_log('!!!17 Синхронизируемые базы совпадают между собой !');
         continue;
         end;
        // Если сервер активен то обновляем
  //if n>22 then
     //showmessage(trim(form1.StringGrid1.Cells[2,n]));
        if (trim(form1.StringGrid1.Cells[0,n])<>'1') then
         if oppgroup or not modeAuto then
          continue;

            gotit:=true;
            form1.StringGrid1.Row:=n;
            //flag_real:=false;
            ConnectINI[4]:=trim(form1.StringGrid1.Cells[3,n]);
            ConnectINI[5]:='5432';
            ConnectINI[6]:=trim(form1.StringGrid1.Cells[4,n]);
            ConnectINI[14]:=trim(form1.StringGrid1.Cells[1,n]);

            //ConnectINI[4]:='172.27.1.5';    //&
            //ConnectINI[6]:='platforma_815';//&
            form1.Label6.Caption:=' Обновляется сервер :'+#13+'               '+trim(ConnectINI[4])+':'+trim(ConnectINI[5])+#13+
                                                              '               '+trim(ConnectINI[6])+' ['+trim(ConnectINI[14])+']';
            //form1.write_log(' ');
            form1.write_log('******************************< '+inttostr(n)+' >***********************************');
            //form1.write_log('Обновляется сервер:  '+trim(ConnectINI[4])+'  '+trim(ConnectINI[6])+' ['+trim(ConnectINI[14])+']');
            form1.write_log('Обновляется сервер '+trim(form1.StringGrid1.Cells[2,n])+' ['+ConnectINI[14]+'] ip:'+ConnectINI[4]);

         //если вести журнал
          if fllog then
            stamptime := form1.logs_sync_start(ConnectINI[14]);
        doit:=true;

         if modeAuto then
          begin
          timecheck:= form1.syncNeedCheck(ConnectINI[14], timetostr(form1.DateTimePicker1.Time));
           if (timecheck <> '01-01-1970 00:00') then
               begin
 //            write_log('['+ConnectINI[14]+'] сервер уже был обновлен с '+(formatdatetime('dd-mm-yyyy',date())+#32+timetostr(form1.DateTimePicker1.Time)));
                 write_log('['+ConnectINI[14]+'] сервер уже был обновлен с '+timecheck);
                 doit:=false;
               end;
         end;
         if doit  then
          begin
              if doit and oppgroup and (form1.syncNeedCheck(ConnectINI[14], inttostr(intervalOpp)+' minutes ') <> '01-01-1970 00:00') then
            begin
             write_log('['+ConnectINI[14]+'] пункт уже был обновлен за последние '+inttostr(intervalOpp)+ ' минут !');
              if not modeAuto then
              showmessage('['+ConnectINI[14]+'] пункт уже был обновлен за последние '+inttostr(intervalOpp)+ ' минут !');
             doit:=false;
            end;
            if doit and (form1.syncNeedCheck(ConnectINI[14], inttostr(intervalTime)+' minutes ') <> '01-01-1970 00:00') then
             begin
             write_log('['+ConnectINI[14]+'] сервер уже был обновлен за последние '+inttostr(intervalTime)+ ' минут !');
            if not modeAuto then
             showmessage('['+ConnectINI[14]+'] сервер уже был обновлен за последние '+inttostr(intervalTime)+ ' минут !');
             doit:=false;
             end;

          end;

       //далее считаем попытки обновления
      if doit and modeAuto then
       begin
            if form1.syncFailsTry(ConnectINI[14]) then
             begin
             write_log('Сервер ['+ConnectINI[14]+' прошло недостаточно времени с момента последней неудачной попытки обновления');
             doit:=false;
             end;
        end;
      if not modeAuto and superuser then doit:=true;

      // Синхронизируем справочники и Создаем локальные списки расписаний
      if doit then
            begin
            //определяем реальный или нет
           if trim(mas_serv_loc[n-1,8])='1' then
             syncOperation(ConnectINI[14],true)
           else
            syncOperation(ConnectINI[14],false);
         end;

      //если вести журнал, запись результата
       if fllog then
           form1.logs_sync_result(ConnectINI[14], stamptime, mlines);
       mlines := form1.Memo1.Lines.Count;
     end;
   end;
   //если не было отмечено ни одного сервера, тогда обновлять из local.ini
   {If superuser and (gotit=false) then
    begin
      // Если IP и name base совпадают с ЦЕНТРАЛЬНЫМ то пропуск
      If (ConnectINI[1]=ConnectINI[4]) and (ConnectINI[3]=ConnectINI[6]) then
       begin
          make_up();
          write_log('IP-адрес и имя локальной БД совпадает с Центральной БД !'+#13+'ОПЕРАЦИЯ БУДЕТ ОТМЕНЕНА !');
          exit;
        end;

          form1.Label6.Caption:=' Обновляется сервер :'+#13+'               '+trim(ConnectINI[4])+':'+trim(ConnectINI[5])+#13+
                                                              '               '+trim(ConnectINI[6])+' ['+trim(ConnectINI[14])+']';
          form1.write_log('Обновляется сервер '+trim(form1.StringGrid1.Cells[2,n])+' ['+ConnectINI[14]+'] ip:'+ConnectINI[4]);
    end;}

   If form1.Memo1.Lines.Count>1 then
    begin
   form1.write_log('--------------/*\ Синхронизация ЗАВЕРШЕНА !  /*\---------------');
   form1.write_log('-----------------\*/______________________\*/------------------');
    end;
   //// Забираем список таблиц
   //if get_table_sync()=false then
   //  begin
   //    exit;
   //  end;
   //application.ProcessMessages;
   //// Копируем обновляемые данные с сервера на сервер
   //if copy_data_sync()=false then
   //  begin
   //    form1.write_log('! ОШИБКА !!!18 ДАЛЬНЕЙШЕЕ ВЫПОЛНЕНИЕ ОПЕРАЦИИ НЕВОЗМОЖНО !!');
   //    exit;
   //  end;
   //application.ProcessMessages;
   //application.ProcessMessages;
   flagSync:=false;
   make_up();
end;


//обновление хранимок
procedure TForm1.Button2Click(Sender: TObject);
 var
  flag:boolean=false;
  n,m:integer;
begin
 // ---- НАЧАЛЬНЫЕ ПРОВЕРКИ ------ //
 if form1.StringGrid1.RowCount<2 then
  begin
    showmessage('В списке нет серверов для обновления !');
    exit;
  end;
 for n:=1 to form1.StringGrid1.RowCount-1 do
    begin
      if trim(form1.StringGrid1.Cells[0,n])='1' then flag:=true;
    end;
 if flag=false then
   begin
    showmessage('В списке не выбрано ни одного сервера для обновления !');
    exit;
   end;
 flag:=false;
 //for n:=1 to form1.StringGrid2.RowCount-1 do
 //   begin
 //     if trim(form1.StringGrid2.Cells[0,n])='1' then flag:=true;
 //   end;
 //if flag=false then
 //  begin
 //   showmessage('В списке не выбрано ни одной процедуры для обновления !');
 //   exit;
 //  end;

 // --- СИНХРОНИЗИРУЕМ ХРАНИМКИ ---- ///

   form1.auto_sync.Enabled:=false;
   form1.Button1.Color:=clGray;
   form1.Memo1.Clear;
   form1.panel1.Visible:=true;
   application.ProcessMessages;

   // Цикл по обновлению серверов по порядку в GRID
   form1.Label6.Caption:='';
   for n:=1 to form1.StringGrid1.RowCount-1 do
      begin
        // Если IP и name base совпадают с ЦЕНТРАЛЬНЫМ то пропуск
        if (trim(form1.StringGrid1.Cells[3,n])=trim(form1.edit1.Text)) and (trim(form1.StringGrid1.Cells[4,n])=trim(form1.edit5.Text)) then continue;

        // Если сервер активен то обновляем
        if trim(form1.StringGrid1.Cells[0,n])='1' then
          begin
            ConnectINI[4]:=trim(form1.StringGrid1.Cells[3,n]);
            ConnectINI[5]:='5432';
            ConnectINI[6]:=trim(form1.StringGrid1.Cells[4,n]);
            ConnectINI[14]:=trim(form1.StringGrid1.Cells[1,n]);
           form1.Label6.Caption:=' Обновляется сервер :'+#13+'               '+trim(ConnectINI[4])+':'+trim(ConnectINI[5])+#13+
                                                            '               '+trim(ConnectINI[6])+' ['+trim(ConnectINI[14])+']';
          form1.write_log('________________________________________________________________________');
          form1.write_log('+++++++++++++++++++++++   '+inttostr(n)+'  +++++++++++++++++++++++++++++');
          form1.write_log('№'+inttostr(n)+'  Обновляется сервер '+trim(form1.StringGrid1.Cells[2,n])+' ['+ConnectINI[14]+'] ip:'+ConnectINI[4]);

            // Синхронизируем схранимые процедуры

            for m:=1 to form1.StringGrid2.RowCount-1 do
              begin
                //showmessage(form1.StringGrid2.Cells[0,m]+' - '+trim(form1.StringGrid2.Cells[1,m]));
                if trim(form1.StringGrid2.Cells[0,m])='1' then
                 updproc(trim(form1.StringGrid2.Cells[1,m]),strtoint(trim(ConnectINI[14])));
            //updproc('aflawless_dohod',strtoint(trim(ConnectINI[14])));
              end;
          end;
      end;

   application.ProcessMessages;
   form1.Panel1.Visible:=false;
   form1.Button1.Color:=clRed;
   application.ProcessMessages;
   form1.auto_sync.Enabled:=true;

end;

procedure TForm1.Button3Click(Sender: TObject);
 var
   n:integer;
begin
  for n:=1 to form1.StringGrid1.RowCount-1 do
     begin
       form1.StringGrid1.Cells[0,n]:='0';
     end;
end;

procedure TForm1.Button4Click(Sender: TObject);
 var
   n:integer;
begin
  for n:=1 to form1.StringGrid1.RowCount-1 do
     begin
       form1.StringGrid1.Cells[0,n]:='1';
     end;

end;

procedure TForm1.Button5Click(Sender: TObject);
 var
   n:integer;
begin
   // Снимаем ВСЕ
   for n:=1 to form1.StringGrid1.RowCount-1 do
     begin
       form1.StringGrid1.Cells[0,n]:='0';
     end;
  // УСТАНАВЛИВАЕМ РЕАЛЬНЫЕ
  for n:=1 to form1.StringGrid1.RowCount-1 do
     begin
       if trim(mas_serv_loc[n-1,8])='1' then  form1.StringGrid1.Cells[0,n]:='1';
     end;
end;

procedure TForm1.Button6Click(Sender: TObject);
 var
   n:integer;
begin
   // Снимаем ВСЕ
   for n:=1 to form1.StringGrid1.RowCount-1 do
     begin
       form1.StringGrid1.Cells[0,n]:='0';
     end;
  // УСТАНАВЛИВАЕМ ВИРТУАЛЬНЫЕ
  for n:=1 to form1.StringGrid1.RowCount-1 do
     begin
       if trim(mas_serv_loc[n-1,8])='0' then  form1.StringGrid1.Cells[0,n]:='1';
     end;
end;

procedure TForm1.Button7Click(Sender: TObject);
begin
  form1.Update_grid;
end;

procedure TForm1.Button8Click(Sender: TObject);
 var
    n:integer;
 begin
    // Снимаем ВСЕ
    for n:=1 to form1.StringGrid2.RowCount-1 do
      begin
        form1.StringGrid2.Cells[0,n]:='1';
      end;
end;

procedure TForm1.Button9Click(Sender: TObject);
 var
    n:integer;
 begin
    // Устанавливаем ВСЕ
    for n:=1 to form1.StringGrid2.RowCount-1 do
      begin
        form1.StringGrid2.Cells[0,n]:='0';
      end;
end;

procedure TForm1.CheckBox1Change(Sender: TObject);
begin
  if form1.CheckBox1.Checked then
   begin
   form1.DateTimePicker1.Enabled:=true;
   form1.CheckBox2.Checked:=false;
   form1.write_log('! ! Синхронизация по расписанию ВКЛЮЧЕНА ! !');
   end
   else
   begin
   form1.DateTimePicker1.Enabled:=false;
   form1.write_log('! В Н И М А Н И Е ! Выключена автоматическая синхронизация данных по расписанию !');
   end;
end;

procedure TForm1.CheckBox2Change(Sender: TObject);
begin
 If form1.CheckBox2.Checked then
  begin
  form1.GroupBox2.Visible:=true;
  form1.CheckBox1.Checked:=false;
  form1.BitBtn1.Enabled:=false;
  form1.Button7.Enabled:=false;
  form1.Edit1.ReadOnly:=false;
  form1.Edit2.ReadOnly:=false;
  form1.Edit5.ReadOnly:=false;
  end
 else
  begin
  form1.GroupBox2.Visible:=false;
  form1.CheckBox1.Checked:=true;
  form1.BitBtn1.Enabled:=true;
  form1.Button7.Enabled:=true;
  form1.Edit1.ReadOnly:=true;
  form1.Edit2.ReadOnly:=true;
  form1.Edit5.ReadOnly:=true;
  end
end;

procedure TForm1.change_interface();
begin
   with form1 do
   begin
     checkbox4.Caption:='';  //удалять дубликаты (быстро)
     checkbox5.Caption:='';  //удалять дубликаты (медленно)
     checkbox6.Caption:=''; //только av_trip таблицы
     checkbox8.Caption:=''; //не создавать av_trip таблицы

  if Form1.CheckBox3.Checked then
   begin
    checkbox3.Caption:='english interface';
    label4.Caption:= 'АРМ Синхронизация баз данных';
    PageControl1.Pages[0].Caption:= '                   Таблицы                 ';
    PageControl1.Pages[1].Caption:= '                   Функции                  ';
    label10.Caption:= 'выбрано серверов';
    label2.Caption:= 'Автоматически:';
    label12.Caption:= 'Вручную';
    groupbox1.Caption:= 'центральный сервер';
    groupbox2.Caption:= 'локальный сервер';
    label5.caption:= 'порт:';
    label7.caption:= 'Имя БД:';
    Stringgrid1.Cells[1,0] := 'ID-пункта';
    Stringgrid1.Cells[2,0] := 'Сервер';
    Stringgrid1.Cells[3,0] := 'IP-адрес';
    Stringgrid1.Cells[5,0] := 'Последняя успешная синхронизация';
    button3.Caption:= 'Снять ВСЕ';
    button4.Caption:= '&Выбрать ВСЕ';
    button5.Caption:= '&Реальные';
    button6.Caption:= '&Виртуальные';
    button7.Caption:= 'Загрузить .ini';
    button1.Caption:= '&ЗАПУСК';
    bitbtn1.Caption:= 'Сохранить .ini';
    bitbtn3.Caption:= '&Выход';
    groupbox3.Caption:= 'опции:';
    label1.caption:= 'удалять дубликаты (быстро)';
    label3.caption:= 'удалять дубликаты (медленно)';
    checkbox7.Caption:='только удалять дубликаты';
    label17.caption:= 'только av_trip таблицы';
    label18.caption:= 'не создавать av_trip таблицы';
   end
  else
  begin
    checkbox3.Caption:='язык русский';
    label4.Caption:= 'Syncronisation and update module';
    PageControl1.Pages[0].Caption:= '                      Tables Sync                 ';
    PageControl1.Pages[1].Caption:= '                    Func & Proc Sync                  ';
    label10.Caption:= 'servers choosen';
    label2.Caption:= 'Auto sync at';
    label12.Caption:= 'Manual sync';
    groupbox1.Caption:= 'central server';
    groupbox2.Caption:= 'local server';
    label5.caption:= 'port:';
    label7.caption:= 'DB-name:';
    Stringgrid1.Cells[1,0] := 'ID point';
    Stringgrid1.Cells[2,0] := 'Point name';
    Stringgrid1.Cells[3,0] := 'IP adress';
    Stringgrid1.Cells[5,0] := 'Last Success Sync';
    button3.Caption:= 'Clear All';
    button4.Caption:= '&Check All';
    button5.Caption:= '&REAL all check';
    button6.Caption:= '&Virtual check all';
    button7.Caption:= 'Pick up from .ini';
    button1.Caption:= '&START    SYNC';
    bitbtn1.Caption:= 'Save .ini';
    bitbtn3.Caption:= '&Exit';
    groupbox3.Caption:= 'options:';
    label1.caption:= 'delete dublicates (fast)';
    label3.caption:= 'delete dublicates (best)';
    checkbox7.Caption:='only delete dublicates';
    label17.caption:= 'only av_trip lists';
    label18.caption:= 'dont create av_trip lists';
  end;
   end;
end;

procedure TForm1.CheckBox3Change(Sender: TObject);
begin
   change_interface();
end;

procedure TForm1.CheckBox6Change(Sender: TObject);
begin
   if form1.CheckBox6.Checked then
     form1.CheckBox8.Checked := false;

end;

procedure TForm1.CheckBox7Change(Sender: TObject);
begin
  if form1.CheckBox7.Checked then
   begin
     //form1.CheckBox4.Checked := true;
     form1.CheckBox5.Checked := true;
   end;
end;

procedure TForm1.CheckBox8Change(Sender: TObject);
begin
     if form1.CheckBox8.Checked then
     form1.CheckBox6.Checked := false;
end;

procedure TForm1.BitBtn1Click(Sender: TObject);
begin
   write_settings(1);
   //write_settings(2);
  form1.write_log('Данные сохранены в platsync_settings.ini ');
end;

procedure TForm1.FormCreate(Sender: TObject);
begin
if  ReadIniLocal(form1.IniPropStorage1,ExtractFilePath(Application.ExeName)+'local.ini')=false then
   begin
     showmessage('Не найден файл настроек по заданному пути!'+#13+'Дальнейшая загрузка программы невозможна !'+#13+'Обратитесь к Администратору !');
     halt;
   end;

id_user := 5;
superuser :=true;

If not FileExistsUTF8(ExtractFilePath(Application.ExeName)+'cheater') then
  begin
   //{$IFDEF WINDOWS}
    superuser:=false;
    id_user:=0;
    //открыть форму регистрации
    FormAuth:=TFormAuth.create(self);
    FormAuth.ShowModal;
    FreeAndNil(FormAuth);
   //{$ENDIF}
   end;

 //id_user := 5;
 //superuser :=false;

If id_user=0 then
 begin
   halt;
   exit;
 end;

//если нужно ограничить
//case id_user of 5,9,11,14,115,410 : oppgroup:=true; end;

// Пишем лейбы центрального сервера
form1.Edit1.Text:=connectini[1];
form1.Edit2.Text:=connectini[2];
form1.Edit5.Text:=connectini[3];

  // Установки даты и времени
 //decimalseparator:='.';
 //DateSeparator := '.';
 //ShortDateFormat := 'dd.mm.yyyy';
 //LongDateFormat  := 'dd.mm.yyyy';
 //ShortTimeFormat := 'hh:mm:ss';
 //LongTimeFormat  := 'hh:mm:ss';


 // Читаем и расставляем данные настроек
 write_settings(0);

 fllog := logs_check_first();
 if not fllog  then
 begin
      //если кто-то из оПП, то без логов нельзя
   if oppgroup then
    begin
       showmessage('Ошибка запроса к истории обновлений!'+#13+'Дальнейшая работа невозможна!'+#13+'Обратитесь к администратору...');
       halt;
     end;
   end;

  // Создаем список серверов
 if form1.create_list_servers(false)=false then
  begin
    write_log('Невозможно получить список серверов с центрального сервера !');
    halt;
    exit;
  end;

 // Создаем список хранимых процедур
  if form1.create_list_proc()=false then
   begin
     write_log('Невозможно получить список хранимых процедур с центрального сервера !');
     halt;
     exit;
   end;
// Обновляем GRID серверов
form1.Update_grid;

// Обновляем GRID хранимых процедур
if superuser then
begin
form1.Update_grid_proc;
end;

//флаг включения таймера автообновлений
//Определяем ip-адрес
flagtimer := form1.getvalidip();

//если айпишник сервака (включен таймер автообновления), тогда не вести журнал
if fllog then
  if (flagtimer) then
    fllog := false;

//включаем таймер
if not oppgroup and not flagtimer then
  form1.auto_sync.Enabled:=true;
end;


procedure TForm1.FormShow(Sender: TObject);
begin
 // запрещено все, если ты не суперюзер, не сервер, опп-шник
if not oppgroup OR superuser OR flagtimer then
 begin
  form1.CheckBox1.visible:=true;
  form1.Label2.visible:=true;
  form1.Button4.Enabled:=true;
  form1.Button5.Enabled:=true;
  form1.Button6.Enabled:=true;
  form1.BitBtn1.Enabled:=true;
 end;

if not oppgroup and flagtimer then
  Form1.CheckBox1.Checked:=true;

if superuser then
  begin
   form1.CheckBox2.visible:=true;
    form1.Label12.visible:=true;
    form1.CheckBox6.Enabled:=true;
    form1.CheckBox3.checked:=false;
    form1.CheckBox7.Enabled:=true;
    form1.CheckBox8.Enabled:=true;
    form1.TabSheet2.enabled:=true;
  end;


//язык интерфейса
  form1.change_interface();

  // Устанавливаем фокус страницы
  form1.PageControl1.ActivePageIndex:=0;
  form1.Button1.SetFocus;
  application.ProcessMessages;

end;


end.

