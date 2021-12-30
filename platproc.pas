unit platproc;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils, ZConnection, ZDataset, LazFileUtils, Controls, Graphics,
  Dialogs, ExtCtrls, StdCtrls,dbf,LConvEncoding,Grids,
  //fileutil,
  Buttons,Forms,IniPropStorage, ComCtrls,LazUtf8,TypInfo,
  {$IFDEF LINUX}
  unix,
  {$ENDIF}
  //libc,
  dateutils,strUtils;

type TBigArray = array of array[0..1] of string;
type Tmas = array of array of string;

procedure DelStringGrid(SGrid:TStringGrid;NRow:Integer);            //Удаляем строки из любого StringGrid
Procedure DelGridColumn(SGrid:TStringGrid;NCol:Integer); //Удаляем колонки из любого StringGrid
procedure MConnect(ZCon:TZConnection; sHost:string; sPort:integer; sBase:string); // Определяет параметры подключения к базе
 function kol_row_file(file_name:string):integer;                    //Возвращает количество строк в текстовом файле
procedure AutoSizeGridColumn(Grid: TStringGrid);                    // Автоподбор ширины столбца
procedure Exchange(List1, List2 : TStrings);                        // Процедура обмена текстовых списков (TStrings)
procedure SetGridFocus(SGrid: TStringGrid; r, c: integer);          //Выделяем фокус ячейки
procedure CentrForm(Frm: TForm);                                    //Выравнивание формы по центру
procedure fillarray;                                                //Запоняет массивы данными названий месяцев и дней недели
 function GetMonthName(nM:byte):string;                                    //Возвращает название месяца
 function GetDayName(nD:byte):string;                                    //Возвращает название дня недели
 function ReadIniLocal(IniObj:TIniPropStorage;inipath:string):boolean;          //возвращает true если доступны настройки серверов
//Procedure SortGrid(Grid: TStringGrid; Num: Integer; Progres:TProgressBar; typ : byte; naprav : byte);                    // //Сортировка Stringgrid
//Procedure SortGrid2(Grid: TStringGrid; Num: Integer; Progres:TProgressBar; typ : byte);  //Сортировка Stringgrid от большего к наименьшему
procedure GridPoisk(Grid:TStringGrid;FindStr:TEdit);                 //Поиск в StringGrid
procedure SetRowColorMenu(Grid:TStringGrid; aCol, aRow: Integer;aRect: TRect); //Раскраска пунктов меню по первому столбцу в StringGrid
//procedure SetMenuGridLocal(Grid:TStringGrid;user_id,user_id_arm:Integer;MyQuery:TZQuery); //Заполнение и доступы к меню LOCAL(начальная загрузка)
 function  PADL(Src: string; AddSrc: string; Lg: Integer): string; // Функция добавляет символами строку до необходимого размера слева
 function  PADR(Src: string; AddSrc: string; Lg: Integer): string; // Функция добавляет символами строку до необходимого размера справа
 function  PADC(Src: string; AddSrc: string; Lg: Integer): string; // Функция добавляет символами строку до необходимого размера слева и справа (центрирует)
procedure ShowEditLog(aForm: TForm; Panel: TPanel; ZQ: TZQuery; ZCon: TZconnection;TableName: string; idnum: string; flag:Byte);// Вывод информации по созданию и редактированию записи
 function GetSymKey(CharS:Char):boolean; //Определяет что нажата буквеено-цифровая клавиша
 function UpperAll(S : String): String; // Преобразовываем в верхний регистр
procedure Canvas_Triangle(Canv:TCanvas;pos_tr:byte;tr_x:integer); //Рисуем треугольник на канве
procedure Click_Header(Grid:TStringGrid;hd_X,hd_Y:integer;hd_progress:TProgressbar); //Обновляем массив текущего Click хидера grid
procedure DrawCell_header(Col:Integer;Canv:TCanvas;Lft:Integer;Grid:TStringgrid); //Рисуем на стрингрид в методе обновления
 function GetIPAddressOfInterface(if_name:string):string;  //Узнаем свой IP
Procedure Delay(Secunds:integer); // Задержка в секундах
 function in_user_sql(arm,appname,user,username,ip_adr:string;ZCon:TZConnection;ZQuery:TZQuery):byte; // Процедура проверки ВХОДА пользователя
 function out_user_sql(ZCon:TZConnection;ZQuery:TZQuery):byte; // Процедура проверки ВЫХОДА пользователя
 function active_user_sql(ZCon:TZConnection; ZQuery:TZQuery; user:string):byte; // Обновление активных файлов пользователя
procedure grid_active_user(Grid:TStringGrid;ZCon:TZConnection;ZQuery:TZquery); //Вывод в GRID активных пользователей
 function DelCheck(Grid:TStringGrid; aCol:integer; ZCon:TZConnection; ZQ:TZquery; sStables:string):byte; //ограничение целостности данных при удалении из таблиц
 function FormatNum(cislo:string;dec:integer):string; //Преобразует числовые поля при вводе в StringGrid в правильный формат 0.00 или 0
                                                     // dec=0 -> 0
                                                     // dec=2 -> 0.00
procedure Showmessagealt(s:string); // Альтернативный ShowMessage
procedure GetSezon(D1: TDateTime; D2: TDateTime; Splan: string; var arSezon: TMas); //определить последовательность дней и признак работы на расписании
function Connect2(ZCon:TZConnection; flProfile: byte):boolean;// ПОПЫТКИ ПОДКЛЮЧЕНИЯ К БАЗЕ
procedure ShowMas(var mas : Tmas); //показать содержимое двумерного массива
function Find_All_Files(BaseDir, ext: string):integer; //****** Поиск всех файлов в папке
function FindAnyFile(BaseDir, ext: string):boolean; //****** Поиск хотя бы одного файла в папке
procedure SearchFiles(APath : String; FExt : String; var ResMas: TBigArray);
procedure GridRowUP(Grid: TStringGrid); //ПЕРЕМЕСТИТЬ СТРОЧКУ ГРИДА ВВЕРХ
procedure GridRowDown(Grid: TStringGrid); //ПЕРЕМЕСТИТЬ СТРОЧКУ ГРИДА ВНИЗ
function get_type_char(keyU:integer):byte;// Цифра или буква (определение нажатой клавиши)
procedure DrawCellsAlign(lgrid:TStringGrid;align_h,align_v:byte;cText:string;gRect:TRect); // Вывод Текстовой строки в ячейке Stringgrid с выравниванием
// align_h - слева=1,центр=2,справа=3
// align_м - вверху=1,центр=2,внизу=3
function ReadIniSale(IniObj:TIniPropStorage;inipath:string):boolean; //возвращает true если доступны настройки кассы
function POSNEXT(stroka:string;substroka:string;nextpos:integer):integer;  // Возвращает позицию подстроки substroka в stroka под номером nextpos
function ip_del_zero(ip:string):string; // Убираем нули из IP адреса
function list_Servers(ZCon:TZConnection; ZQuery:TZQuery;idpoint:integer;user_id:integer):boolean; //Возвращает список серверов доступных для данного
                                                                                                  // остановочного пункта и текущего пользователя
//procedure set_server(flag:string); // Устанавливает окружение для нового сервера
                                   // flag - remote or local

var
  ConnectINI: array [1..17] of String;
  My_Header:array of integer; //массив колонок грида с направлением сортировки
  empty_name:string;
  id_tek_user:integer;
  ar_report : array of array of string;
  Sale_INI: array [1..17] of String;
  Tek_server,Real_server:integer;  //Текущий и локальный сервер
    mas_otkuda:array of array of string; //Массив доступных серверов
  //mas_otkuda[length(mas_otkuda)-1,0]:=ZQuery.FieldByName('id').asString;
  //mas_otkuda[length(mas_otkuda)-1,1]:=ZQuery.FieldByName('name').asString;
  //mas_otkuda[length(mas_otkuda)-1,2]:=ZQuery.FieldByName('ip').asString;
  //mas_otkuda[length(mas_otkuda)-1,3]:=ZQuery.FieldByName('ip2').asString;
  //mas_otkuda[length(mas_otkuda)-1,4]:=ZQuery.FieldByName('base_name').asString;
  //mas_otkuda[length(mas_otkuda)-1,5]:=ZQuery.FieldByName('login').asString;
  //mas_otkuda[length(mas_otkuda)-1,6]:=ZQuery.FieldByName('pwd').asString;
  //mas_otkuda[length(mas_otkuda)-1,7]:=ZQuery.FieldByName('port').asString;
  //mas_otkuda[length(mas_otkuda)-1,8]:=ZQuery.FieldByName('local').asString;
  cUser:string='platforma';
  cPass:string='19781985';



const
// Параметры подключения к базе (постоянные)
 //cPort = 5432;
 //cUser = 'platforma';
 //cPass = '19781985';
 cMezhgorod = 'Междугородный';
 cPrigorod  = 'Пригородный';
 cKray = 'Межобластной';
 cGos ='Межгосударственный';
 cNCon = 5;  //кол-во попыток подключения к базе
 IP_NAMESIZE = 16;

type
 THackGrid = class(TStringGrid);
 ipstr = array[0..IP_NAMESIZE-1] of char;


implementation

uses
  main;

var
  namemonth: Array [1..12] of String;
    nameday: Array[1..7] of String;


////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////
//                     Процедуры и Функции системы                        //
////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////
// Устанавливает окружение для нового сервера
// flag - remote or local
procedure set_server(flag:string);
 var
     n:integer;
begin
 if trim(flag)='remote' then
  begin
    for n:=0 to length(mas_otkuda)-1 do
      begin
        if trim(mas_otkuda[n,0])=trim(inttostr(Tek_Server)) then
         begin
         // Переопределяем переменные на новый сервер
         ConnectINI[14]:=mas_otkuda[n,0];
         ConnectINI[4]:=mas_otkuda[n,3];
         ConnectINI[5]:=mas_otkuda[n,7];
         ConnectINI[6]:=mas_otkuda[n,4];
         cUser:=mas_otkuda[n,5];
         cPass:=mas_otkuda[n,6];
         break;
        end;
      end;
    end;

 if trim(flag)='local' then
  begin
        // Переопределяем переменные на новый сервер
        ConnectINI[4]:=mas_otkuda[0,3];
        ConnectINI[5]:=mas_otkuda[0,7];
        ConnectINI[6]:=mas_otkuda[0,4];
        ConnectINI[14]:=mas_otkuda[0,0];
        cUser:=mas_otkuda[0,5];
        cPass:=mas_otkuda[0,6];
    end;

  //showmessage(ConnectINI[4]+#13+
  //            ConnectINI[5]+#13+
  //            ConnectINI[6]+#13+
  //                    cUser+#13+
  //                    cPass+#13);


  //mas_otkuda[length(mas_otkuda)-1,0]:=ZQuery.FieldByName('id').asString;
  //mas_otkuda[length(mas_otkuda)-1,1]:=ZQuery.FieldByName('name').asString;
  //mas_otkuda[length(mas_otkuda)-1,2]:=ZQuery.FieldByName('ip').asString;
  //mas_otkuda[length(mas_otkuda)-1,3]:=ZQuery.FieldByName('ip2').asString;
  //mas_otkuda[length(mas_otkuda)-1,4]:=ZQuery.FieldByName('base_name').asString;
  //mas_otkuda[length(mas_otkuda)-1,5]:=ZQuery.FieldByName('login').asString;
  //mas_otkuda[length(mas_otkuda)-1,6]:=ZQuery.FieldByName('pwd').asString;
  //mas_otkuda[length(mas_otkuda)-1,7]:=ZQuery.FieldByName('port').asString;
  //mas_otkuda[length(mas_otkuda)-1,8]:=ZQuery.FieldByName('local').asString;
  //ConnectINI[4]:=IniObj.ReadString('ip_local','нет значения');
  //ConnectINI[5]:=IniObj.ReadString('port_local','0');
  //ConnectINI[6]:=IniObj.ReadString('name_base_local','нет значения');
  //ConnectINI[7]:=IniObj.ReadString('load_1c','нет значения');
  //ConnectINI[14]:=IniObj.ReadString('id_point_local','0');
  //ConnectINI[15]:=IniObj.ReadString('Auth_server','LOCAL');
end;

//Возвращает список серверов доступных для данного
//остановочного пункта и текущего пользователя
function list_Servers(ZCon:TZConnection; ZQuery:TZQuery;idpoint:integer;user_id:integer):boolean;
 var
     n:integer;
begin
   if length(mas_otkuda)>0 then
      begin
        result:=true;
        exit;
      end;
    // -------------------- Соединяемся с локальным сервером ----------------------
   If not(Connect2(Zcon,2)) then
    begin
     showmessagealt('Соединение с сервером базы данных отсутствует !'+#13+'Проверьте сетевое соединение и опции файла настроек системы...');
     Result:=false;
     exit;
    end;
   // Заправшиваем все доступные сервера
   ZQuery.sql.Clear;
   ZQuery.sql.add('select *  from getservers('+quotedstr('servers')+','+inttostr(idpoint)+','+inttostr(user_id)+');');
   ZQuery.sql.add('FETCH ALL IN servers;');
   //showmessage(zquery.SQL.Text);
  try
      ZQuery.open;
  except
         showmessagealt('Нет данных по СЕРВЕРАМ ПРОДАЖ !!!'+#13+'Сообщите об этом АДМИНИСТРАТОРУ !!!');
         ZQuery.Close;
         Zcon.disconnect;
         result:=false;
         exit;
  end;

  if ZQuery.RecordCount=0 then
     begin
       showmessagealt('Нет данных по СЕРВЕРАМ ПРОДАЖ !!!'+#13+'Сообщите об этом АДМИНИСТРАТОРУ !!!');
       ZQuery.Close;
       Zcon.disconnect;
       result:=false;
       exit;
     end;

  // Заполняем массив СЕРВЕРОВ
  SetLength(mas_otkuda,0,0);
  for n:=0 to ZQuery.RecordCount-1 do
    begin
      SetLength(mas_otkuda,length(mas_otkuda)+1,9);
      if n>0 then
         begin
           mas_otkuda[length(mas_otkuda)-1,0]:=ZQuery.FieldByName('id').asString;
           mas_otkuda[length(mas_otkuda)-1,1]:=ZQuery.FieldByName('name').asString;
           mas_otkuda[length(mas_otkuda)-1,2]:=ip_del_zero(ZQuery.FieldByName('ip').asString);
           mas_otkuda[length(mas_otkuda)-1,3]:=ip_del_zero(ZQuery.FieldByName('ip2').asString);
           mas_otkuda[length(mas_otkuda)-1,4]:=ZQuery.FieldByName('base_name').asString;
           mas_otkuda[length(mas_otkuda)-1,5]:=ZQuery.FieldByName('login').asString;
           mas_otkuda[length(mas_otkuda)-1,6]:=ZQuery.FieldByName('pwd').asString;
           mas_otkuda[length(mas_otkuda)-1,7]:=ZQuery.FieldByName('port').asString;
           mas_otkuda[length(mas_otkuda)-1,8]:=ZQuery.FieldByName('local').asString;
           ZQuery.Next;
         end;
      if n=0 then
         begin
          //ConnectINI[4]:=IniObj.ReadString('ip_local','нет значения');
          //ConnectINI[5]:=IniObj.ReadString('port_local','0');
          //ConnectINI[6]:=IniObj.ReadString('name_base_local','нет значения');
          //ConnectINI[7]:=IniObj.ReadString('load_1c','нет значения');
          //ConnectINI[14]:=IniObj.ReadString('id_point_local','0');
          //ConnectINI[15]:=IniObj.ReadString('Auth_server','LOCAL');
          mas_otkuda[length(mas_otkuda)-1,0]:=ConnectINI[14];
          mas_otkuda[length(mas_otkuda)-1,1]:=ZQuery.FieldByName('name').asString;
          mas_otkuda[length(mas_otkuda)-1,2]:=ConnectINI[4];
          mas_otkuda[length(mas_otkuda)-1,3]:=ConnectINI[4];
          mas_otkuda[length(mas_otkuda)-1,4]:=ConnectINI[6];
          mas_otkuda[length(mas_otkuda)-1,5]:=cUser;
          mas_otkuda[length(mas_otkuda)-1,6]:=cPass;
          mas_otkuda[length(mas_otkuda)-1,7]:=ConnectINI[5];
          mas_otkuda[length(mas_otkuda)-1,8]:=ZQuery.FieldByName('local').asString;
          ZQuery.Next;
         end;
    end;

  ZQuery.Close;
  Zcon.disconnect;

  // Определяем текущий по умолчанию сервер
  Real_Server:=strtoint(mas_otkuda[0,0]);
  result:=true;
end;


// Убираем нули из IP адреса
function ip_del_zero(ip:string):string;
 var
   ip_r:string='';
begin
   ip:=trim(ip);
   ip_r:=inttostr(strtoint(trim(UTF8Copy(ip,1,POSNEXT(ip,'.',1)-1))))+'.';
   ip_r:=ip_r+inttostr(strtoint(trim(UTF8Copy(ip,POSNEXT(ip,'.',1)+1,POSNEXT(ip,'.',2)-POSNEXT(ip,'.',1)-1))))+'.';
   ip_r:=ip_r+inttostr(strtoint(trim(UTF8Copy(ip,POSNEXT(ip,'.',2)+1,POSNEXT(ip,'.',3)-POSNEXT(ip,'.',2)-1))))+'.';
   ip_r:=ip_r+inttostr(strtoint(trim(UTF8Copy(ip,POSNEXT(ip,'.',3)+1,3))));
   result:=ip_r;
end;




function POSNEXT(stroka:string;substroka:string;nextpos:integer):integer;  // Возвращает позицию подстроки substroka в stroka под номером nextpos
 var
   n:integer;
   kol:integer=0;
begin
  Result:=0;
  for n:=1 to UTF8Length(stroka) do
    begin
      if UTF8Copy(stroka,n,1)=trim(substroka) then kol:=kol+1;
      if kol=nextpos then
        begin
         Result:=n;
         exit;
        end;
    end;
end;


function ReadIniSale(IniObj:TIniPropStorage;inipath:string):boolean;  //возвращает true если доступны настройки кассы
begin
       // Считываем sale.ini
      if FileExistsUTF8(inipath) then
        begin
            IniObj.inifilename:=inipath;
            IniObj.IniSection:='SETTINGS FR'; //указываем секцию
            Sale_INI[1]:=IniObj.ReadString('port','');
            Sale_INI[2]:=IniObj.ReadString('speed','');
            result:=true;
        end
      else
        begin
           // Если нет sale.ini то создаем по умолчанию
           IniObj.inifilename:=ExtractFilePath(Application.ExeName)+'sale.ini';
           IniObj.IniSection:='SETTINGS FR'; //указываем секцию
           IniObj.WriteString('port','/dev/ttyS0');
           IniObj.WriteString('speed','6');
           Sale_INI[1]:='/dev/ttyS0';
           Sale_INI[2]:='6';
           result:=true;
        end;
end;

//----------------------------------------------------------------------------------------------------//
// ПОПЫТКИ ПОДКЛЮЧЕНИЯ К БАЗЕ
function Connect2(ZCon:TZConnection; flProfile: byte):boolean;
var
    i : integer;
    flwait:boolean=false;
begin
  result := false;
  //showmessage('iweireiruie');
  //ЦЕНТРАЛЬНЫЙ СЕРВЕР РЕАЛЬНЫЙ
  if flProfile=1 then
     begin
     Zcon.hostname:=ConnectINI[1];
     try
        Zcon.port:=strToInt(ConnectINI[2]);
      except
         on exception: EConvertError do
         begin
          showmessagealt('ОШИБКА КОНВЕРТАЦИИ !!!'+#13+'НЕВЕРНОЕ ЗНАЧЕНИЕ ПОРТА !');
          exit;
         end;
      end;
     Zcon.Database:=ConnectINI[3];
     end;
  //ЛОКАЛЬНЫЙ СЕРВЕР РЕАЛЬНЫЙ
  if flProfile=2 then
    begin
    Zcon.hostname:=ConnectINI[4];
    try
        Zcon.port:=strToInt(ConnectINI[5]);
      except
         on exception: EConvertError do
         begin
          showmessagealt('ОШИБКА КОНВЕРТАЦИИ !!!'+#13+'НЕВЕРНОЕ ЗНАЧЕНИЕ ПОРТА !');
          exit;
         end;
      end;
     Zcon.Database:=ConnectINI[6];
     end;
  //ЦЕНТРАЛЬНЫЙ СЕРВЕР ТЕСТОВЫЙ
    if flProfile=3 then
     begin
     Zcon.hostname:=ConnectINI[8];
     try
        Zcon.port:=strToInt(ConnectINI[9]);
      except
         on exception: EConvertError do
         begin
          showmessagealt('ОШИБКА КОНВЕРТАЦИИ !!!'+#13+'НЕВЕРНОЕ ЗНАЧЕНИЕ ПОРТА !');
          exit;
         end;
      end;
     Zcon.Database:=ConnectINI[10];
     end;
  //ЛОКАЛЬНЫЙ СЕРВЕР ТЕСТОВЫЙ
  if flProfile=4 then
     begin
     Zcon.hostname:=ConnectINI[11];
     try
        Zcon.port:=strToInt(ConnectINI[12]);
      except
         on exception: EConvertError do
         begin
          showmessagealt('ОШИБКА КОНВЕРТАЦИИ !!!'+#13+'НЕВЕРНОЕ ЗНАЧЕНИЕ ПОРТА !');
          exit;
         end;
      end;
     Zcon.Database:=ConnectINI[13];
     end;

   Zcon.user:=cUser;
   Zcon.password:=cPass;
   //пробуем подключиться
   //flwait:=false;
   //if zcon.PingServer() then showmessage(Zcon.hostname+'   true') else showmessage(Zcon.hostname+'   false');
   //if zcon.PingServer() then
   // begin
    zcon.Properties.Add('timeout=5');
     try
         ZCon.connect;
          //ZCon.Connected := True;
    except
      On E: Exception do
      begin
        //ZCon.Connected := False;
        //showmessage('Cannot connect to the database at the moment. Please try again later.');
       exit;
      end;
     end;
    //end;
    //else
    //  begin
    //   flwait:=true;
    //  end;
  //если с первого раза не удалось
  //If flwait then
  // begin
  // //не получилось подключиться с первого раза открываем окно
  //  FormIdle:=TFormIdle.create(form1);
  //  FormIdle.Height:=160;
  //  FormIdle.width:=form1.Width;
  //  FormIdle.Left:=form1.Left;
  //  FormIdle.top:=form1.top+(form1.Height div 2)-(FormIdle.Height div 2);
  //  FormIdle.Show;
  //  info := 'ПОДОЖДИТЕ ! Идет подключение к серверу... ';
  //  flclose:=false;
  //  for i:=1 to cNCon do
  //   begin
  //   If i>1 then Delay(2); //ожидаем 2 секунды
  //    try
  //       ZCon.connect;
  //    except
  //       continue;
  //    end;
  //    result:=true;
  //    break;
  //  end;
  // freeandnil(FormIdle);
  // exit;
  //end;
  //

   //if zcon.Connected=true then showmessage('TRUE') else showmessage('FALSE');
 if zcon.Connected=true then result:=true else result:=false;
end;
//----------------------------------------------------------------------------------------------------//

//----------------------------------------------------------------------------------------------------//
// Определяет параметры подключения к базе
procedure MConnect(ZCon:TZConnection; sHost:string; sPort:integer; sBase:string);
begin
   Zcon.port:= sPort;
   Zcon.Database:=sBase;
   Zcon.user:=cUser;
   Zcon.password:=cPass;
   Zcon.hostname:=sHost;
end;
//----------------------------------------------------------------------------------------------------//

//----------------------------------------------------------------------------------------------------//
// Чтение значений настроек серверов SQL
function ReadIniLocal(IniObj:TIniPropStorage;inipath:string):boolean;
begin
        if FileExistsUTF8(inipath) then
           begin
            IniObj.inifilename:=inipath;
            IniObj.IniSection:='CENTRAL SERVER SQL'; //указываем секцию
            ConnectINI[1]:=IniObj.ReadString('ip_central','нет значения');
            ConnectINI[2]:=IniObj.ReadString('port_central','0');
            ConnectINI[3]:=IniObj.ReadString('name_base','нет значения');

            IniObj.IniSection:='LOCAL SERVER SQL'; //указываем секцию
            ConnectINI[4]:=IniObj.ReadString('ip_local','нет значения');
            ConnectINI[5]:=IniObj.ReadString('port_local','0');
            ConnectINI[6]:=IniObj.ReadString('name_base_local','нет значения');

            IniObj.IniSection:='EMU SERVER SQL'; //указываем секцию
            ConnectINI[8]:=IniObj.ReadString('ip_central','нет значения');
            ConnectINI[9]:=IniObj.ReadString('port_central','0');
            ConnectINI[10]:=IniObj.ReadString('name_base','нет значения');
            ConnectINI[11]:=IniObj.ReadString('ip_local','нет значения');
            ConnectINI[12]:=IniObj.ReadString('port_local','0');
            ConnectINI[13]:=IniObj.ReadString('name_base_local','нет значения');
            IniObj.IniSection:='LOCAL SETTINGS'; //указываем секцию
            ConnectINI[7]:=IniObj.ReadString('load_1c','нет значения');
            ConnectINI[14]:=IniObj.ReadString('id_point_local','0');
            ConnectINI[15]:=IniObj.ReadString('Auth_server','LOCAL');
            result:=true;
           end
        else
           begin
             result:=false;
           end;
end;
//----------------------------------------------------------------------------------------------------//

// Цифра или буква (определение нажатой клавиши)
function get_type_char(keyU:integer):byte;
// 1-цифра
// 2-буква
begin
  result:=0;
  if (chr(keyU) in ['0'..'9']) then Result:=1;
  if (chr(keyU) in ['A'..'Z','a'..'z',#186,#188,#190,#219,#222]) then Result:=2;
    //showmessage(inttostr(result));
end;

//*********************************** Вывод Текстовой строки в ячейке Stringgrid с выравниванием
procedure DrawCellsAlign(lgrid:TStringGrid;align_h,align_v:byte;cText:string;gRect:TRect); // Вывод Текстовой строки в ячейке Stringgrid с выравниванием
 // align_h - слева=1,центр=2,справа=3
 // align_м - вверху=1,центр=2,внизу=3
  var
    h,w,n:integer;
    xc,yc:integer;
    s1,s2:string;
    kolslow:integer=0;
    mas_slow:array of string;
 begin
  // --------------Учитываем переносы
  // Подсчитываем количество слов в предложении
  kolslow:=1;
  SetLength(mas_slow,0);
  for n:=1 to UTF8Length(ctext) do
     begin
       if UTF8copy(cText,n,1)='*' then
          begin
           SetLength(mas_slow,length(mas_slow)+1);
           mas_slow[length(mas_slow)-1]:=UTF8copy(cText,kolslow,n-kolslow);
           kolslow:=n;
          end;
     end;

   if (length(mas_slow)>0) and (kolslow<UTF8Length(cText)) then
     begin
      SetLength(mas_slow,length(mas_slow)+1);
      mas_slow[length(mas_slow)-1]:=UTF8copy(cText,kolslow+1,UTF8Length(cText)-kolslow);
     end;

   if length(mas_slow)=0 then
     begin
        SetLength(mas_slow,1);
        mas_slow[0]:=trim(ctext);
     end;

  //-------Выводим кадое слово строки с соответствующим выравниванием
  for n:=0 to length(mas_slow)-1 do
    begin
      w:=lgrid.Canvas.TextWidth(mas_slow[n]);
      h:=lgrid.Canvas.TextHeight(mas_slow[n]);
      case align_h of
        1:xc:=gRect.Left+2;
        2:xc:=gRect.Left+((gRect.Right-gRect.Left) div 2)-(w div 2);
        3:xc:=gRect.Left+(gRect.Right-w-2);
      end;
      case align_v of
        1:yc:=gRect.top+(n*h);
        2:yc:=gRect.top+((gRect.bottom-gRect.top) div 2)-(h div 2);
        3:yc:=(gRect.bottom-(h*(n+1))-2);
//        gRect.top+
      end;
      IF not(length(mas_slow)=1) then yc:=gRect.top+2+(n*h);
      lGrid.Canvas.TextOut(xc,yc,mas_slow[n]);
    end;
 end;



//***********************      ПЕРЕМЕСТИТЬ СТРОЧКУ ГРИДА ВНИЗ            *******************************
procedure GridRowDown(Grid: TStringGrid);
var
  i: integer;
begin
  With Grid do
  begin
 // Если нет пунктов или последний то ничего не делаем
  if (Row=RowCount-1) or (RowCount<2) then exit;
  //Меняем местами строчки грида
    begin
       RowCount := RowCount+1;
       for i:=1 to Colcount-1 do
       begin
       Cells[i,RowCount-1]:=Cells[i,Row];
       Cells[i,Row]:=Cells[i,Row+1];
       Cells[i,Row+1]:=Cells[i,RowCount-1];
       end;
       RowCount := RowCount-1;
    end;
  SetFocus;
  Row:=Row+1;
end;
end;


//****************    ПЕРЕМЕСТИТЬ СТРОЧКУ ГРИДА ВВЕРХ  ********************************
procedure GridRowUP(Grid: TStringGrid);
var
  i:integer;
begin
  with Grid do
begin
// Если нет пунктов или первый то ничего не делаем
  if (Row=1) or (RowCount<2) then exit;
  //Меняем местами строчки грида
    begin
       RowCount := RowCount+1;
       for i:=1 to Colcount-1 do
       begin
       Cells[i,RowCount-1]:=Cells[i,Row];
       Cells[i,Row]:=Cells[i,Row-1];
       Cells[i,Row-1]:=Cells[i,RowCount-1];
       end;
       RowCount := RowCount-1;
    end;
  SetFocus;
  Row:=Row-1;
end;
end;

//*********************************************************                 поиск файлов  *********************************
procedure SearchFiles(APath : String; FExt : String; var ResMas: TBigArray);
 //const
   //SearchMasks : Array [0..3] Of String = ('*.doc','*.xls','*.txt','*.rtf');
 var
   iFound : Integer;
   F : TSearchRec;
   sRes : TStringlist;
   sExt : string;
  //  function IsMatchMask(AFileName : String) : Boolean;
  //var
  //  I : Integer;
  //begin
  //  Result := False;
  //  For I := Low(SearchMasks) To High(SearchMasks) Do
  //    Begin
  //      Result := LazFileUtils.MatchesMask(AFileName,SearchMasks[i]);
  //      If Result Then Break;
  //    End;
  //end;

 begin
  APath := IncludeTrailingPathDelimiter(APath);
 // sRes.Clear;
   while ansipos(';',FExt)>0 do
   begin
    sExt := '*.'+copy(FExt,1,ansipos(';',FExt)-1);
    If trim(sExt)='*.' then sExt := '*.*';
    //showmessage(sExt);
    //sRes := fileutil.FindAllFiles(APath,sExt,true);
   //iFound := FindFirst(APath + '*.*',faAnyFile,F);
   //While iFound = 0 Do
   //  Begin
   //    If (F.Attr And faDirectory) <> 0
   //      Then
   //        Begin
   //          If (F.Name <> '.') And (F.Name <> '..')
   //            Then SearchFiles(APath + F.Name);
   //        End
   //      Else
   //        If IsMatchMask(F.Name)
   //          Then Memo1.Lines.Add(APath + F.Name);
   //    iFound := FindNext(F);
   //  End;
   //FindClose(F);
   //showmessage(inttostr(sRes.Count)+#13+ sRes.Text);
   FExt := copy(FExt,ansipos(';',FExt)+1,length(Fext));
   end;
 end;


//****** Поиск хотя бы одного файла в папке *****************************************************
function FindAnyFile(BaseDir, ext: string):boolean;
 Var fs : TSearchRec;
     iKol : Integer;
     Dir: string;
 begin
 //Проверка на директорию
  if  (BaseDir='') or (not DirectoryExists(BaseDir)) then Result:=false; //Нет папки
  //Добавляем черту, если надо
  Dir := IncludeTrailingPathDelimiter(BaseDir);      //для Win ='\' Linux='/'

 { iKol:=FindFirst(Dir+ext,faAnyFile, fs);
   while iKol=0 do
      begin
       //Form1.ListBox1.Items.Add(fs.Name); // Добавляем файл в список
       iKol := FindNext(fs);
      end;       }
  If FindFirst(Dir+ext,faAnyFile, fs)=0  then
     result:=true
  else result:=false;
  FindClose(fs);
 end;

//****** Поиск всех файлов в папке *********************************************
 function Find_All_Files(BaseDir, ext: string):integer;
 Var fs : TSearchRec;
     iKol : Integer;
     Dir: string;
 begin
 //Проверка на директорию
  if  (BaseDir='') or (not DirectoryExists(BaseDir)) then
     begin
       Result:=0; //Нет папки
       exit;
     end;
  //Добавляем черту, если надо
  Dir := IncludeTrailingPathDelimiter(BaseDir);      //для Win ='\' Linux='/'

   iKol:=FindFirst(Dir+ext,faAnyFile, fs);
   if iKol=0 then
      begin
        repeat
          inc(iKol);
          //Form1.ListBox1.Items.Add(fs.Name); // Добавляем файл в список
        until FindNext(fs)<>0;
      end;
 FindClose(fs);
 Result:=iKol;
 end;


procedure ShowMas(var mas : Tmas); //показать содержимое двумерного массива
var
  n,m:integer;
  s:string='';
  tmp:string='';
  //fl:boolean;
begin
  for n:=low(mas) to high(mas) do
     begin
       //fl:=false;
       tmp:='';
      for m:=low(mas[low(mas)]) to high(mas[low(mas)]) do
         begin
           tmp := tmp + ' | ' + mas[n,m];
         end;
      s:=s+'**'+tmp+#13;
     end;
  showmessageALT(inttostr(length(mas))+#13+s);
end;

//******************** определить последовательность дней и признак работы на расписании  *******************
procedure GetSezon(D1: TDateTime; D2: TDateTime; Splan: string; var arSezon: TMas);// TBigArray);
var
    Y, M, D : Word;
    n,nW,nDw,period,sdvig : integer;
    tekDate : TDateTime;
begin
  If length(Splan)<61 then
     begin
       showmessagealt('Ошибка! Неправильное значение параметров!');
       exit;
     end;
  IF D2<D1 then
     begin
       showmessagealt('Ошибка! Дата конца периода больше даты его начала!');
       exit;
     end;
  //tekDate := D1+1;
  tekDate := D1;
  for n:=0 to DaysBetween(D2,D1)-1 do
  begin
   //добавляем элемент в конечный массив
    SetLength(arSezon,length(arSezon)+1,2);
    arSezon[length(arSezon)-1,0] := dateToStr(tekDate+n);
    arSezon[length(arSezon)-1,1] := '0';
    DecodeDate(tekDate+n, Y, M, D);
  // Проверяем что месяц удовлетворяет
   IF splan[M]='0' then continue;
   //Категория числа месяца
   if splan[13]='1' then
      begin
     // Проверяем на доступные числа
      IF sPlan[D+29]='1' then arSezon[length(arSezon)-1,1]:='1';
       //showmessage('date '+dateTostr(tekdate)+' month '+inttostr(m)+' day '+inttostr(n-29));
      end;
  // Категория-  Осуществление перевозки каждый n день\ Порядок первозчиков выполения расписания
    if sPlan[61]<>'0' then
      begin
        try
          period:= strtoint(sPlan[61]);
        except
          on exception: EConvertError do
           begin
            showmessagealt('Ошибка корветрации !'+#13+'ConvertError');
             break;
             end;
        end;
        try
          sdvig:= strtoint(sPlan[62]);
        except
          on exception: EConvertError do
           begin
            showmessagealt('Ошибка корветрации !'+#13+'ConvertError');
             break;
             end;
        end;
       //первый день выхода= дате актвации
       If (n=0) AND (sdvig=1) then
         begin
           arSezon[length(arSezon)-1,1]:='1';
           continue;
         end;
       //определяем выход
       If (DaysBetween(D1+sdvig-1,tekDate+n) mod period)=0  then
         arSezon[length(arSezon)-1,1]:='1';
       end;

    //Категория по неделям, четным, нечетным дням
     if sPlan[15]='1' then
      begin
       //определяем какой день недели 1 число и вычисляем сколько прибавлять
       nDw := DayOftheWeek(EncodeDate( Y, M, 1));
     //  showmessage('Day '+datetostr(Dt)+' : '+inttostr(nDw));
       nDw := nDw - 1;
      // nDw := DayOfWeek(tekDate); //порядок дня недели
      // nW := (D - nDw -1) div 7 + 1; //номер недели месяца
       nW := (D+nDw) div 7;
       If ((D+nDw) mod 7)<>0 then
          nW := nW + 1;
     //   showmessage('Day '+datetostr(tekdate)+' nDw '+inttostr(nDw)+' nW '+inttostr(nW));
         // Проверка что нужная неделя
        If ((sPlan[16]='1') and (nW=1) or (nW=5)) or
           ((sPlan[17]='1') and (nW=2)) or
           ((sPlan[18]='1') and (nW=3)) or
           ((sPlan[19]='1') and (nW=4)) then
            begin
             //День недели
             If ((sPlan[20]='1') and (nDW=1)) or
                ((sPlan[21]='1') and (nDW=2)) or
                ((sPlan[22]='1') and (nDW=3)) or
                ((sPlan[23]='1') and (nDW=4)) or
                ((sPlan[24]='1') and (nDW=5)) or
                ((sPlan[25]='1') and (nDW=6)) or
                ((sPlan[26]='1') and (nDW=7)) then
                    begin  //Четное\Нечетное
                     //Все
                     if (sPlan[27]='1') then  arSezon[length(arSezon)-1,1]:='1';
                     //Четные
                     if (sPlan[28]='1') and not(Odd(D)) then arSezon[length(arSezon)-1,1]:='1';
                     //Нечетный
                     if (sPlan[29]='1') and Odd(D) then arSezon[length(arSezon)-1,1]:='1';
                    end;
            end;
      end;
  end;
end;


//Преобразует числовые поля при вводе в StringGrid в правильный формат 0.00 или 0
function FormatNum(cislo:string;dec:integer):string;
begin
 // Формат 0
  if dec=0 then
    begin
      cislo:=StringReplace(cislo,' ','',[rfReplaceAll]);
      if trim(cislo)='' then cislo:='0';
    end;

 // Формат 0.00
  if dec=2 then
    begin
      cislo:=StringReplace(cislo,' ','',[rfReplaceAll]);
      // Добавляем строку до 2 нулей после запятой
      if pos('.',trim(cislo))>0 then
        begin
         cislo:=copy(trim(cislo),1,pos('.',trim(cislo))-1)+'.'+padr(copy(trim(cislo),pos('.',trim(cislo))+1,100),'0',2);
        end;

      // Добавляем 0 до запятой если пусто
      if (trim(copy(trim(cislo),1,pos('.',trim(cislo))-1))='') and (pos('.',trim(cislo))>0) then
          begin
           cislo:='0'+trim(cislo);
          end;
      // Если число вида 21 то делаем 21.00
      if pos('.',trim(cislo))<1 then
          begin
           cislo:=trim(cislo)+'.00';
          end;

    end;
  Result:=cislo;
end;

//******************      ограничение целостности данных при удалении из таблиц   *****************************************
function DelCheck(Grid:TStringGrid; aCol:integer; ZCon:TZConnection; ZQ:TZquery; sStables:string):byte;
var
    ar1 : array of array of string;
    k, n: integer;
    tmpS,sTable,sCol,sZav, smess : string;

begin
   IF trim(sStables)='' then exit;
   k:= 0;
   SetLength(ar1, 0, 2);
   result:=0;
   //разбираем строчку с перечнем таблиц и полей для проверки
   while pos(',',sStables)>0 do
   begin
    //inc(k);
    //SetLength(ar1, Length(ar1)+1, 2);
  //  ar1[k-1, 0] := copy(sStables, pos(',',sStables)+1, Length(sStables)-pos(',',sStables)-1);
    tmpS := copy(sStables, 1, pos(',',sStables)-1);
    If pos('.',tmpS)<1 then
      begin
       sStables := copy(sStables, pos(',', sStables)+1, Length(sStables));
       continue;
      end;
    sCol := '';//искомый аттрибут
    sTable := '';//зависимая таблица
    sZav := ''; //зависимый аттрибут
    sTable := copy(tmpS , 1, pos('.',tmpS)-1);
    smess :=''; //сообщение на вывод

    If pos(':',tmpS)>0 then
      begin
      sCol := copy(tmpS, pos('.',tmpS)+1, pos(':',tmpS)-1-pos('.',tmpS));
      sZav := copy(tmpS, pos(':',tmpS)+1, Length(tmpS)-1);
      end
    else
     sCol := copy(tmpS, pos('.',tmpS)+1, Length(tmpS)-1);
     sStables := copy(sStables, pos(',', sStables)+1, Length(sStables));
    //showmessagealt('Table : '+sTable+'  Col : '+sCol);

    If trim(sCol)='' then continue;
    If trim(sTable)='' then continue;

 // Подключаемся к серверу
   If not(Connect2(Zcon, flagProfile)) then
     begin
      showmessagealt('Соединение с основным сервером потеряно !'+#13+'Проверьте соединение и/или'+#13+' обратитесь к администратору.');
      exit;
     end;

   IF trim(sZav)='' then
     TmpS := sCol
     else
       TmpS := sZav;
 // Запрос на НАЛИЧИЕ ЗАПИСЕЙ В СВЯЗАННЫХ ТАБЛИЦАХ
 ZQ.SQL.clear;
 ZQ.SQL.Add('SELECT '+TmpS+' FROM '+ sTable +' WHERE del=0 AND '+ sCol +'='+ Grid.Cells[aCol,Grid.Row] + ';');
 //showmessage(ZQ.SQL.Text);
  try
     ZQ.open;
 except
    showmessage('SQL Запрос - ОШИБКА !' +#13 + ZQ.SQL.Text);
    //Zcon.Disconnect;
    result:=0;
    break;
 end;
 //если есть связанные записи
 if ZQ.RecordCount>0 then
    begin
     for n:=1 to ZQ.RecordCount do
       begin
       smess := smess + ZQ.FieldByName(TmpS).asString +',  ';
       ZQ.Next;
       end;
 If trim(sZav)<>'' then
   begin
    ZQ.SQL.clear;
    ZQ.SQL.add('SELECT a.relname,b.attname, b.attnum, c.description FROM pg_class AS a ');
    ZQ.SQL.add('JOIN pg_attribute AS b ON b.attrelid = a.oid AND b.attnum>0 AND b.attisdropped=False ');
    ZQ.SQL.add('LEFT JOIN pg_description AS c ON c.objoid = a.oid AND c.objsubid=b.attnum ');
    ZQ.SQL.add('WHERE a.relname='+ QuotedSTR(sTable) + ' AND b.attname=' + QuotedSTR(sZav)+ ';');
    try
       ZQ.Open;
    except
       showmessage('Выполнение команды SQL - ОШИБКА !'+#13+'Команда: '+ZQ.SQL.Text);
       exit;
    end;
   sZav := ZQ.FieldByName('description').asString;
   end;

  //запрос  описания искомой таблицы
    ZQ.SQL.clear;
    ZQ.SQL.add('SELECT distinct c.description FROM pg_class AS a ');
    ZQ.SQL.add('LEFT JOIN pg_description AS c ON c.objoid = a.oid AND c.objsubid=0 ');
    ZQ.SQL.add('WHERE trim(a.relname) ='+QuotedStr(sTable) + ';');
    try
       ZQ.Open;
    except
       showmessage('Выполнение команды SQL - ОШИБКА !'+#13+'Команда: '+ZQ.SQL.Text);
       exit;
    end;
      if ZQ.RecordCount=1 then
         sTable := ZQ.FieldByName('description').asString;
      If trim(sZav)='' then
        showmessagealt('Операция НЕВОЗМОЖНА !'+#13+'Данная запись участвует в таблице:'+#13+' <<'+sTable+'>>'+#13+'и должна быть удалена прежде в ней !')
      else
        showmessagealt('Операция НЕВОЗМОЖНА !'+#13+'Сначала удалите данную запись в таблице:'+#13+'<<'+sTable+'>>,'+#13+'в которой < '+sZav+' > равно значениям:'+#13+smess);

       result:=0;
       break;
    end
 else
    result:=1;
 end;

 //SetLength(ar1,0,0);
 //ar1 := nil;
end;

//----------------------------------------------------------------------------------------------------//
//  Вывод в GRID активных пользователей
procedure grid_active_user(Grid:TStringGrid;ZCon:TZConnection;ZQuery:TZquery);
 var
  tmp_list:array of array of string;
  tmp_list2:array of array of string;
  n,m:integer;
  mflag:byte;
begin
  //--------------------------Кто имеет запись присутствия в системе ---------------------------------------
 // Подключаемся к серверу
   If not(Connect2(Zcon, flagProfile)) then
     begin
      showmessagealt('Соединение с основным сервером потеряно !'+#13+'Проверьте соединение и/или'+#13+' обратитесь к администратору.');
      exit;
     end;
 // Запрос на существующие записи пользователей
 Zquery.SQL.clear;
 Zquery.SQL.Add('SELECT * from av_active_user;');
  try
     Zquery.open;
 except
     ZCon.Disconnect;
    exit;
 end;
 if ZQuery.RecordCount=0 then exit;
 // Пробуем пользователей
 setlength(tmp_list,ZQuery.RecordCount,5);
 for n:=0 to ZQuery.RecordCount-1 do
   begin
     tmp_list[n,0]:=ZQuery.FieldByName('namearm').asString;
     tmp_list[n,1]:=ZQuery.FieldByName('name').asString;
     tmp_list[n,2]:=ZQuery.FieldByName('ipuser').asString;
     tmp_list[n,3]:=ZQuery.FieldByName('createdatetime').asString;
     tmp_list[n,4]:=ZQuery.FieldByName('id').asString;
     ZQuery.Next;
   end;
 for n:=0 to Length(tmp_list)-1 do
   begin
     Zquery.SQL.clear;
     Zquery.SQL.Add('INSERT INTO av_active_user(id) VALUES ('+trim(tmp_list[n,4])+');');
     try
       Zquery.ExecSQL;
     except
       ZCon.Disconnect;
       exit;
     end;
   end;
 //Ждем 1 секунду
 delay(1);
 //Собираем все что осталось
 Zquery.SQL.clear;
 Zquery.SQL.Add('SELECT * from av_active_user where trim(name)='+quotedstr('')+';');
  try
     Zquery.open;
 except
     ZCon.Disconnect;
     exit;
 end;

// showmessagealt('wgqregtegw');

 if ZQuery.RecordCount=0 then exit;
 setlength(tmp_list2,ZQuery.RecordCount,1);
 for n:=0 to ZQuery.RecordCount-1 do
   begin
     tmp_list2[n,0]:=ZQuery.FieldByName('id').asString;
     ZQuery.Next;
   end;
 Zquery.SQL.clear;
 Zquery.SQL.Add('DELETE from av_active_user where trim(name)='+quotedstr('')+';');
  try
     Zquery.ExecSQL;
 except
   ZCon.Disconnect;
   exit;
 end;
 ZCon.Disconnect;
 Grid.RowCount:=1;
 //Выясняем неиспользованные записи и рисуем GRID
 for n:=0 to length(tmp_list)-1 do
   begin
    mflag:=0;
    for m:=0 to length(tmp_list2)-1 do
      begin
        if tmp_list[n,4]=tmp_list2[m,0] then mflag:=1;
      end;
    if mflag=0 then
       begin
        //Рисуем строку GRID
        grid.RowCount:=grid.RowCount+1;
        grid.Cells[0,grid.RowCount-1]:=tmp_list[n,0];
        grid.Cells[1,grid.RowCount-1]:=tmp_list[n,1];
        grid.Cells[2,grid.RowCount-1]:=tmp_list[n,2];
        grid.Cells[3,grid.RowCount-1]:=tmp_list[n,3];
       end;
   end;
 SetLength(tmp_list,0,0);
 tmp_list := nil;
 SetLength(tmp_list2,0,0);
 tmp_list2 := nil;
end;


//----------------------------------------------------------------------------------------------------//
// Обновление активных файлов пользователя
function active_user_sql(ZCon:TZConnection;ZQuery:TZQuery; user:string):byte;
var
    ipserver,namefileip,parsstr:string;
    pfile2:textfile;
begin
 // Проверяем что есть сеть
 //showmessagealt('Пинг пошел !');
 if flagprofile=1 then ipserver:=trim(ConnectINI[1]);
 if flagprofile=2 then ipserver:=trim(ConnectINI[4]);
 if flagprofile=3 then ipserver:=trim(ConnectINI[8]);
 if flagprofile=4 then ipserver:=trim(ConnectINI[11]);
 namefileip:='/tmp/pingip';
{$IFDEF LINUX}
 fpsystem('ping  -c 1 '+ipserver+' > '+namefileip);
{$ENDIF}
 if FileExistsUTF8(namefileip)=false then
    begin
      Result:=1;
      exit;
    end;
 // Парсим файл вывода PING
assignfile(pfile2,namefileip);
if  FileExistsUTF8(namefileip)=true then
   begin
      reset(pfile2);
   end
  else
    begin
      Result:=1;
      exit;
    end;

 // Разбираем строку
 while not eof(pfile2) do
 begin
  readln(pfile2,parsstr);
  if pos('100% packet loss',parsstr)>0 then
     begin
       Closefile(pfile2);
       Result:=1;
       exit;
     end;
 end;
 Closefile(pfile2);

    //--------------------------Удаляем входящих ---------------------------------------
   // Подключаемся к серверу
   If not(Connect2(Zcon, flagProfile)) then
     begin
      //showmessagealt('Соединение с основным сервером потеряно !'+#13+'Проверьте соединение и/или'+#13+' обратитесь к администратору.');
      Result:=1;
      exit;
     end;
   //Сканируем и если надо удаляем
   ZQuery.SQL.clear;
   ZQuery.SQL.add('SELECT id FROM av_active_user where trim(name)='+quotedstr('')+' and id='+user+';');
   try
      ZQuery.open;
   except
      ZCon.Disconnect;
      Result:=1;
      exit;
   end;
   //Если есть входящие с моим ID то удаляем
   if ZQuery.RecordCount>0 then
    begin
      ZQuery.SQL.clear;
      ZQuery.SQL.add('DELETE FROM av_active_user where id='+user+' AND trim(name)='+quotedstr('')+';');
      try
         ZQuery.ExecSQL;
      except
         ZCon.Disconnect;
         Result:=1;
         exit;
      end;
    end;
   ZCon.Disconnect;
   result:=0;
end;
//----------------------------------------------------------------------------------------------------//


// Процедура проверки выхода пользователя
function out_user_sql(ZCon:TZConnection;ZQuery:TZQuery):byte;
begin
  //--------------------------Пишем свой ID---------------------------------------
   // Подключаемся к серверу
   If not(Connect2(Zcon, flagProfile)) then
     begin
      //showmessagealt('Соединение с основным сервером потеряно !'+#13+'Проверьте соединение и/или'+#13+' обратитесь к администратору.');
      Result:=1;
      exit;
     end;
   // Запрос на редактируюмую запись
    ZQuery.SQL.clear;
    ZQuery.SQL.add('UPDATE av_login_log SET outdatetime=now() where id=' +inttostr(id_tek_user)+';');
    try
        ZQuery.ExecSQL;
    except
       ZCon.Disconnect;
       Result:=1;
      //showmessagealt('Соединение с сервером базы данных отсутствует !'+#13+'Проверьте сетевое соединение и опции файла настроек системы...');
     exit;
    end;
    ZCon.Disconnect;
    Result:=1;
end;
//-----------------------------------------------------------------------------------------------------//


//----------------------------------------------------------------------------------------------------//
// Процедура проверки входа пользователя
function in_user_sql(arm,appname,user,username,ip_adr:string;ZCon:TZConnection;ZQuery:TZQuery):byte;
 var
  tek_id:integer;
begin
  //--------------------------Пишем свой ID---------------------------------------
   // Подключаемся к серверу
   If not(Connect2(Zcon, flagProfile)) then
     begin
      showmessagealt('Соединение с основным сервером потеряно !'+#13+'Проверьте соединение и/или'+#13+' обратитесь к администратору.');
      Result:=1;
      exit;
     end;
   // Запрос на редактируюмую запись
    ZQuery.SQL.clear;
    ZQuery.SQL.add('INSERT INTO av_active_user (id) VALUES ('+user+');');
    try
        ZQuery.ExecSQL;
    except
       ZCon.Disconnect;
       Result:=1;
      //showmessagealt('Соединение с сервером базы данных отсутствует !'+#13+'Проверьте сетевое соединение и опции файла настроек системы...');
     exit;
    end;
   // Задерживаем проверку
   delay(1);
   // Проверям свою запись на удаление
   ZQuery.SQL.clear;
   ZQuery.SQL.add('SELECT id FROM av_active_user where trim(name)='+quotedstr('')+' and id='+user+';');
   //showmessagealt(ZQuery.SQL.text);
   try
      ZQuery.open;
   except
      ZCon.Disconnect;
      Result:=1;
      //showmessagealt('Соединение с сервером базы данных отсутствует !'+#13+'Проверьте сетевое соединение и опции файла настроек системы...');
      exit;
   end;

   // Если свою запись не удалили то можно входить
   if ZQuery.RecordCount>0 then
      begin
           // Удаляем свои записи
           ZQuery.SQL.clear;
           ZQuery.SQL.add('DELETE FROM av_active_user where id='+user+';');
           try
              ZQuery.ExecSQL;
           except
              Result:=1;
              ZCon.Disconnect;
              //showmessagealt('Соединение с сервером базы данных отсутствует !'+#13+'Проверьте сетевое соединение и опции файла настроек системы...');
              exit;
           end;

           // Пишем свою запись
           ZQuery.SQL.clear;
           ZQuery.SQL.add('INSERT INTO av_active_user(id, name, namearm, arm, ipuser, createdatetime) VALUES (');
           ZQuery.SQL.add(user+','+quotedstr(trim(username))+','+quotedstr(trim(arm))+','+quotedstr(trim(appname)));
           ZQuery.SQL.add(','+quotedstr(ip_adr)+','+quotedstr(FormatDateTime('dd-mm-yyyy',date)+'_'+FormatDateTime('hh-mm-ss',time)));
           ZQuery.SQL.add(');');
           try
              ZQuery.ExecSQL;
           except
              Result:=1;
              ZCon.Disconnect;
              //showmessagealt('Соединение с сервером базы данных отсутствует !'+#13+'Проверьте сетевое соединение и опции файла настроек системы...');
              exit;
           end;
           //===================== Пишем свою запись в ЛОГ av_login_log ======================
           // Ищем id arm
           ZQuery.SQL.clear;
           ZQuery.SQL.add('SELECT  id from av_arm where trim(armapp)='+quotedstr(trim(appname))+';');
           try
              ZQuery.open;
           except
              Result:=1;
              ZCon.Disconnect;
              //showmessagealt('Соединение с сервером базы данных отсутствует !'+#13+'Проверьте сетевое соединение и опции файла настроек системы...');
              exit;
           end;

           tek_id:=ZQuery.FieldByName('id').asInteger;
           ZQuery.SQL.clear;
           ZQuery.SQL.add('INSERT INTO av_login_log(indatetime, ipuser, id_user, id_arm) VALUES (now(),');
           ZQuery.SQL.add(quotedstr(ip_adr)+','+user+','+trim(inttostr(tek_id)));
           ZQuery.SQL.add(');');
           try
              ZQuery.ExecSQL;
           except
              Result:=1;
              ZCon.Disconnect;
              exit;
           end;
           // Возвращаем текущий id
           ZQuery.SQL.clear;
           ZQuery.SQL.add('SELECT max(id) as id from av_login_log;');
           try
              ZQuery.Open;
              id_tek_user:=Zquery.FieldByName('id').asInteger;
           except
              Result:=1;
              ZCon.Disconnect;
              id_tek_user:=0;
              exit;
           end;

           // Закрываем соединение
           ZCon.Disconnect;
           Result:=0;
           exit;
      end;
     // Если удалили, то входить нельзя
     ZCon.Disconnect;
     Result:=1;
end;
//----------------------------------------------------------------------------------------------------//


//----------------------------------------------------------------------------------------------------//
// Задержка в секундах
Procedure Delay(Secunds:integer);
var i:integer;
begin
for i:=0 to Secunds*10 do
  begin
    sleep(100);
    application.processmessages;
  end;
end;
//----------------------------------------------------------------------------------------------------//


//----------------------------------------------------------------------------------------------------//
//Узнаем свой IP
function GetIPAddressOfInterface(if_name:string):string;
//var
//  ifr : ifreq;
//  tmp:ipstr;
//  sock : longint;
//  p:pChar;
//
begin
//  Result:='0.0.0.0';
//  strncpy( ifr.ifr_ifrn.ifrn_name, pChar(if_name), IF_NAMESIZE-1 );
//  ifr.ifr_ifru.ifru_addr.sa_family := AF_INET;
//  FillChar(tmp[0], IP_NAMESIZE, #0);
//  sock := socket(AF_INET, SOCK_DGRAM, IPPROTO_IP);
//  if ( sock >= 0 ) then begin
//    if ( ioctl( sock, SIOCGIFADDR, @ifr ) >= 0 ) then begin
//      p:=inet_ntoa( ifr.ifr_ifru.ifru_addr.sin_addr );
//      if ( p <> nil ) then strncpy(tmp, p, IP_NAMESIZE-1);
//      if ( tmp[0] <> #0 ) then Result :=  tmp;
//    end;
//    libc.__close(sock);
//  end;
end;
//----------------------------------------------------------------------------------------------------//


//----------------------------------------------------------------------------------------------------//
//Рисуем треугольник на канве
procedure Canvas_Triangle(Canv:TCanvas;pos_tr:byte;tr_x:integer);
 var
  arPxl: array [0..2]  of TPoint;
 begin
  // Рисуем треугольник
  Canv.pen.Width:=3;
  Canv.pen.Color:=clGreen;
  Canv.Brush.Color:=clGreen;
  // Треугольник вниз
  if pos_tr=1 then
     begin
      arPxl[0].X:=5+tr_x;
      arPxl[0].Y:=5;
      arPxl[1].X:=10+tr_x;
      arPxl[1].Y:=10;
      arPxl[2].X:=15+tr_x;
      arPxl[2].Y:=5;
     end;
  // Треугольник вверх
  if pos_tr=2 then
     begin
      arPxl[0].X:=5+tr_x;
      arPxl[0].Y:=10;
      arPxl[1].X:=10+tr_x;
      arPxl[1].Y:=5;
      arPxl[2].X:=15+tr_x;
      arPxl[2].Y:=10;
     end;
  Canv.Polygon(arPxl);
end;
//----------------------------------------------------------------------------------------------------//


//----------------------------------------------------------------------------------------------------//
//Рисуем на стрингрид в методе обновления
procedure DrawCell_header(Col:Integer;Canv:TCanvas;Lft:Integer;Grid:TStringgrid);
begin
  if length(My_Header)=0 then SetLength(My_Header,Grid.ColCount);
  //если другой грид (с другим числом колонок)
  If Grid.ColCount<>length(My_header) then
    begin
      SetLength(My_Header,0);
      SetLength(My_Header,Grid.ColCount);
    end;
  if My_Header[Col]=1 then Canvas_Triangle(canv,1,Lft);
  if My_Header[Col]=2 then Canvas_Triangle(canv,2,Lft);
end;
//----------------------------------------------------------------------------------------------------//


//----------------------------------------------------------------------------------------------------//
//Обновляем массив текущего Click хидера grid
procedure Click_Header(Grid:TStringGrid;hd_X,hd_Y:integer;hd_progress:TProgressbar);
 var
  n,Col,Row,flag : integer;
  numcol : integer;
  //s:string;
begin
   Grid.MouseToCell(hd_X,hd_Y,col,row);
   if row>0 then exit;
   //определяем тип колонки (числовая или текстовая)
   numcol :=0; //текстовая
   try
    numcol:= strtoint(trim(grid.Cells[col,1])); //проверка числовой колонки
   except
      on Exception : EConvertError do numcol:=0;
   end;
   If numcol > 0 then numcol:=1;

   // Меняем значения массивов
  flag:=0;
  for n:=low(My_Header) to high(My_Header) do
      begin
        if ((My_Header[n]=0) or (My_Header[n]=2)) and (n=Col) and (flag=0) then
           begin
            My_Header[n]:=1;
            flag:=1;
           end;
        if (My_Header[n]=1) and (n=Col) and (flag=0) then
           begin
             My_Header[n]:=2;
             flag:=1;
           end;
        if not(n=Col) then My_Header[n]:=0;
      end;
  // Сортировка в правильном порядке
  //if My_Header[Col]>0 then SortGrid(Grid,col,hd_Progress,numcol, My_Header[Col]);
//  if My_Header[Col]=2 then SortGrid2(Grid,col,hd_Progress,numcol);
end;
//----------------------------------------------------------------------------------------------------//


//----------------------------------------------------------------------------------------------------//
 //Определяет что нажата буквенно-цифровая клавиша
 function GetSymKey(CharS:Char):boolean;
 begin
   Result := CharS in ['A'..'Z','a'..'z','0'..'9',#186,#188,#190,#219,#222];
 end;
 //----------------------------------------------------------------------------------------------------//


 //----------------------------------------------------------------------------------------------------//
// Преобразовываем в верхний регистр
 function UpperAll(S : String): String;
 const
    SL = 'abcdefghijklmnopqrstuvwxyzабвгдеёжзийклмнопрстуфхцчшщъыьэюя';
    SU = 'ABCDEFGHIJKLMNOPQRSTUVWXYZАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ';
 var
      N,M : Byte;
      ss:string='';
      flag:boolean=false;
 begin
   for N := 1 to Utf8Length(S) do
     begin
       for m:=1 to  UTF8Length(SL) do
         begin
           if UTF8Copy(S,n,1)=UTF8Copy(SL,m,1) then
            begin
             ss:=ss+UTF8Copy(SU,m,1);
             flag:=true;
            end;
         end;
       if flag=false then ss:=ss+UTF8Copy(S,n,1);
     end;
   UpperAll := ss;
end;
 //----------------------------------------------------------------------------------------------------//



 //----------------------------------------------------------------------------------------------------//
// Вывод информации по созданию и редактированию записи
procedure ShowEditLog(aForm: TForm; Panel: TPanel; ZQ: TZQuery; ZCon: TZconnection;TableName: string; idnum: string; flag:Byte);
var
    tmp_id_first,tmp_id,datCreate,datEdit,ktoCreate,ktoCreate2,ktoEdit,ktoEdit2: string;
    seredina,kolEdit: integer;
Const
   T1 ='Служебная информация записи:';
   T2 ='Дата редактирования: ';
   T3 ='Количество редакций: ';
   T4 ='Последний редактор:  ';
   //T5 ='последний:';
   T6 ='  Дата создания: ';
   T7 ='  Создал: ';
begin
   // Подключаемся к серверу
   If not(Connect2(Zcon, flagProfile)) then
     begin
      showmessagealt('Соединение с основным сервером потеряно !'+#13+'Проверьте соединение и/или'+#13+' обратитесь к администратору.');
      exit;
     end;

   aForm.Repaint;
   Panel.BorderStyle:=bsSingle;
   Panel.Caption:='';
   seredina:= (Panel.Width div 2)-Panel.Canvas.TextWidth(T2);
   //Проверяем возможность выполнения флага 1
   if seredina<15 then
      flag:=0;
   // Подключаемся к серверу
   try
      Zcon.connect;
   except
     showmessagealt('Соединение с сервером базы данных отсутствует !'+#13+'Проверьте сетевое соединение и опции файла настроек системы...');
     exit;
   end;
  // Запрос на редактируюмую запись
  ZQ.SQL.clear;
  ZQ.SQL.add('Select createdate_first,createdate,id_user_first,id_user FROM '+TableName+' WHERE id='+idnum+' AND del=0');
  try
    ZQ.Open;
    except
       showmessagealt('Выполнение команды SQL - ОШИБКА !'+#13+'Команда: '+ZQ.SQL.Text);
    end;
    If ZQ.RecordCount<0 then exit;

    datCreate :=  trim(ZQ.FieldByName('createdate_first').asString);//дата создания
    datEdit :=  trim(ZQ.FieldByName('createdate').asString);//дата редактирования
    tmp_id_first := ZQ.FieldByName('id_user_first').asString; //id_user_first
    tmp_id :=  ZQ.FieldByName('id_user').asString; //id_user

   //сколько раз редактировался
    ZQ.SQL.Clear;
    ZQ.SQL.add('SELECT count(*) FROM av_spr_kontragent WHERE del=1 AND id='+idnum+';');
    try
        ZQ.open;
    except
       showmessagealt('Выполнение команды SQL - ОШИБКА !'+#13+'Команда: '+ZQ.SQL.Text);
    end;
   If ZQ.RecordCount>0 then
    kolEdit :=  ZQ.FieldByName('count').asInteger;

   ////кто редактировал
    ZQ.SQL.Clear;
    ZQ.SQL.add('Select fullname,dolg from av_users where id='+tmp_id+';');
    try
        ZQ.open;
    except
       showmessagealt('Выполнение команды SQL - ОШИБКА !'+#13+'Команда: '+ZQ.SQL.Text);
    end;
    If  ZQ.RecordCount>0 then
    ktoEdit:=trim(ZQ.FieldByName('fullname').asString)+', ';
    ktoEdit2:= trim(ZQ.FieldByName('dolg').asString);

    //Если показывать инфу создания
   If flag=1 then
    begin
      ktoEdit:=copy(ktoEdit,1,seredina);
      ktoEdit2:=copy(ktoEdit2,1,seredina);
      //кто создал
    ZQ.SQL.Clear;
    ZQ.SQL.add('Select fullname,dolg from av_users where id='+tmp_id_first+';');
    try
        ZQ.open;
    except
       showmessagealt('Выполнение команды SQL - ОШИБКА !'+#13+'Команда: '+ZQ.SQL.Text);
    end;
    If ZQ.RecordCount>0 then
    ktoCreate:=trim(ZQ.FieldByName('fullname').asString)+', ';
    ktoCreate2:=trim(ZQ.FieldByName('dolg').asString);
    end;

    //рисуем
    with Panel do
    begin
     Canvas.Font.Color:=clWindowText;
     Canvas.Font.Size:=10;
     Canvas.Font.Style:=Canvas.FOnt.style + [fsItalic];
     Canvas.TextOut(20,1,T1);
     Canvas.Font.Style:=Canvas.FOnt.style - [fsItalic];
     If flag=1 then
    begin
     Canvas.TextOut(6,23,T2+datEdit);
     Canvas.TextOut(6,43,T3+IntToSTR(kolEdit));
     Canvas.TextOut(6,63,T4+ktoEdit);
     //Canvas.TextOut(30,77,T5);
     //Canvas.Font.Style:=Canvas.Font.style + [fsBold];
     //Canvas.TextOut(Canvas.TextWidth(T2)+6,23,datEdit);
     //Canvas.TextOut(Canvas.TextWidth(T3)+6,43,IntToSTR(kolEdit));
     //Canvas.TextOut(Canvas.TextWidth(T4)+10,63,ktoEdit);
     Canvas.TextOut(Canvas.TextWidth(T4),77,ktoEdit2);
    end;
     //Если показывать инфу создания
   If flag=1 then
    begin
     Canvas.TextOut(6,23,T2+datEdit+T6+datCreate);
     Canvas.TextOut(6,43,T3+IntToSTR(kolEdit));
     Canvas.TextOut(6,63,T4+ktoEdit+T7+ktoCreate);
     Canvas.TextOut(Canvas.TextWidth(T4),77,ktoEdit2);
     Canvas.TextOut(Canvas.TextWidth(T2+datEdit+T6),77,ktoCreate2);
    end;
   end;
    ZQ.Close;
    Zcon.disconnect;
end;
//----------------------------------------------------------------------------------------------------//



//----------------------------------------------------------------------------------------------------//
// Функция добавляет символами строку до необходимого размера слева
function PADL(Src: string; AddSrc: string; Lg: Integer): string;
 begin
  Result := trim(Src);
  while UTF8Length(Result) < Lg do
   Result := AddSrc + Result;
 end;
//----------------------------------------------------------------------------------------------------//



//----------------------------------------------------------------------------------------------------//
// Функция добавляет символами строку до необходимого размера  справа
function PADR(Src: string; AddSrc: string; Lg: Integer): string;
 begin
  Result := trim(Src);
  while UTF8Length(Result) < Lg do
   Result := Result+AddSrc;
 end;
//----------------------------------------------------------------------------------------------------//

//----------------------------------------------------------------------------------------------------//
// Функция добавляет символами строку до необходимого размера слева и справа (центрирует)
function  PADC(Src: string; AddSrc: string; Lg: Integer): string;
begin
 Result := Src;
 while UTF8Length(Result) < Lg do
 begin
   Result := Result + AddSrc;
   if UTF8Length(Result) < Lg then
   begin
     Result := AddSrc + Result;
   end;
 end;
end;
//----------------------------------------------------------------------------------------------------//




//----------------------------------------------------------------------------------------------------//
//Раскраска пунктов меню по первому столбцу в StringGrid
 procedure SetRowColorMenu(Grid:TStringGrid; aCol, aRow: Integer;aRect: TRect);
 begin
   if ((aCol=0) or (aCol=1)) and (trim(grid.Cells[aCol,aRow])='2') then
      begin
        grid.Canvas.Brush.Color := clGreen;
        grid.Canvas.FillRect(aRect);
      end;
   if ((aCol=0) or (aCol=1)) and (trim(grid.Cells[aCol,aRow])='1') then
    begin
        grid.Canvas.Brush.Color := clYellow;
        grid.Canvas.FillRect(aRect);
    end;
   if ((aCol=0) or (aCol=1)) and (trim(grid.Cells[aCol,aRow])='0') then
    begin
        grid.Canvas.Brush.Color := clRed;
        grid.Canvas.FillRect(aRect);
    end;
 end;
//----------------------------------------------------------------------------------------------------//



//----------------------------------------------------------------------------------------------------//
////Заполнение и доступы к меню LOCAL(начальная загрузка)
//procedure SetMenuGridLocal(Grid:TStringGrid;user_id,user_id_arm:Integer;MyQuery:TZQuery);
// var
//   n:integer;
//begin
//  // Соединяемся с сервером и выбираем список доступных меню для пользователя + АРМ
//  Grid.RowCount:=MyQuery.RecordCount;
//  for n:=1 to MyQuery.RecordCount do
//    begin
//     grid.Cells[2,n-1]:=MyQuery.FieldByName('id_menu_loc').AsString;
//     grid.Cells[1,n-1]:=MyQuery.FieldByName('Name').AsString;
//     grid.Cells[0,n-1]:=MyQuery.FieldByName('permition').AsString;
//     MyQuery.Next;
//    end;
//end;
//----------------------------------------------------------------------------------------------------//

// Альтернативный ShowMessage
procedure Showmessagealt(s:string);
 var
  i:integer;
  MyMesDlg:TForm;
  //MyComponent:TButton;
  E:TBitBtn;
begin
  // Программно создаем модальную форму
  MyMesDlg := CreateMessageDialog(trim(s), mtCustom, [mbYes]);
  //MyMesDlg.Color := clMoneyGreen;
  MyMesDlg.BorderIcons:=[];
  MyMesDlg.BorderStyle:=bsDialog;
  MyMesDlg.BorderWidth:=0;
  MyMesDlg.Font.Color := clBlack;
  MyMesDlg.Caption:='В Н И М А Н И Е !';
  for i:=0 to MyMesDlg.ComponentCount-1 do
      begin
       if (IsPublishedProp(MyMesDlg.Components[i],'Caption')) then
           begin
             E:= TBitBtn(MyMesDlg.Components[i]);
             E.Width:=100;
             if E.Caption='&Yes' then E.Caption:='Принять';
           end;
       end;
  if MyMesDlg.ShowModal = 7 then
       begin
         exit;
       end;
  FreeAndNil(MyMesDlg);
end;


//----------------------------------------------------------------------------------------------------//
// Процедура Контекстный Поиск в гриде
procedure GridPoisk(Grid:TStringGrid;FindStr:TEdit);
 var
   found: byte;
  n,m,i:integer;
  MyMesDlg:TForm;
  //MyComponent:TButton;
  //dlgButton:TButton;
  //TComponent;
  E:TBitBtn;
  //s:string;
begin
  found:=0;
  Grid.row:=1;
        for m:=Grid.col to Grid.ColCount-1 do
        begin
         // m:=Grid.Col;
            for n:=1 to Grid.RowCount-1 do
            begin
           // Без звездочки в конце
           if (pos(upperall(trim(FindStr.text)),upperall(Grid.Cells[m,n]))>0) and not(copy(trim(FindStr.text),length(trim(FindStr.text))-1,1)='*') then
                 begin
                   //Найдено
                   found:=1;
                   SetGridFocus(Grid,n,m);
                   MyMesDlg := CreateMessageDialog('Продолжить поиск ?', mtConfirmation, mbYesNo);
                   //MyMesDlg.Color := clMoneyGreen;
                   MyMesDlg.Font.Color := clBlack;
                   MyMesDlg.Caption:='Выберите вариант ?';

                   for i:=0 to MyMesDlg.ComponentCount-1 do
                    begin
                     if (IsPublishedProp(MyMesDlg.Components[i],'Caption')) then
                           begin
                            E:= TBitBtn(MyMesDlg.Components[i]);
                            if E.Caption='&Yes' then E.Caption:='Да';
                            if E.Caption='&No' then E.Caption:='Нет';
                           end;
                    end;
                    if MyMesDlg.ShowModal = 7 then
                     begin
                       exit;
                     end;
                   FreeAndNil(MyMesDlg);
                 end;

           // Со звездочкой в конце
           if (upperall(trim(FindStr.text))=upperall(trim(Grid.Cells[m,n]))) and (copy(trim(FindStr.text),length(trim(FindStr.text))-1,1)='*') then
                 begin
                   //Найдено
                   found:=1;
                   SetGridFocus(Grid,n,m);
                   MyMesDlg := CreateMessageDialog('Продолжить поиск ?', mtConfirmation, mbYesNo);
                   //MyMesDlg.Color := clMoneyGreen;
                   MyMesDlg.Font.Color := clBlack;
                   MyMesDlg.Caption:='Выберите вариант ?';
                   for i:=0 to MyMesDlg.ComponentCount-1 do
                    begin
                     if (IsPublishedProp(MyMesDlg.Components[i],'Caption')) then
                           begin
                            E:= TBitBtn(MyMesDlg.Components[i]);
                            if E.Caption='&Yes' then E.Caption:='Да';
                            if E.Caption='&No' then E.Caption:='Нет';
                           end;
                    end;
                    if MyMesDlg.ShowModal = 7 then
                     begin
                       exit;
                     end;
                    FreeAndNil(MyMesDlg);
                 end;
            end;
        end;

        If found=0 then showmessagealt('Совпадений НЕ найдено!');
end;
//----------------------------------------------------------------------------------------------------//



//----------------------------------------------------------------------------------------------------//
//Сортировка Stringgrid
Procedure SortGrid(Grid: TStringGrid; Num: Integer; Progres:TProgressBar; typ: byte; naprav : byte);
Var
    I,J,K :   Integer;
Begin
  Grid.Color:=clSilver;
  Progres.Max:=Grid.RowCount;
  Progres.Position:=0;
  Progres.visible:=true;
   FOR I:=1 TO Grid.RowCount-1 DO
    begin
      Progres.Position:=Progres.Position+1;
      Progres.refresh;
      K:=I;    // Первоначально принимаем, что строка с минимальным элементом - верхняя
        FOR J:= I+1 TO Grid.RowCount-1 DO
        begin
         //если числовая колонка
         if typ>0 then
          begin
            try
              strtoint(Grid.Cells[Num, K])
            except
               continue;
            end;
            try
              strtoint(Grid.Cells[Num, J])
            except
               continue;
            end;
            //направление сортировки
            If naprav=1 then
             begin
              IF strtoint(Grid.Cells[Num, K]) > strtoint(Grid.Cells[Num, J]) THEN // Ищем строку с минимальным элементом
                K:=J;
              end
            else
              IF strtoint(Grid.Cells[Num, K]) < strtoint(Grid.Cells[Num, J]) THEN // Ищем строку с максимальным элементом
                K:=J;
          end;
         //если текстовая колонка
         if typ=0 then
          begin
            //направление сортировки
            If naprav=1 then
             begin
               IF UPPERALL(Grid.Cells[Num, K]) > UPPERALL(Grid.Cells[Num, J]) THEN // Ищем строку с минимальным элементом
                K:=J;
             end
            ELSE
               IF UPPERALL(Grid.Cells[Num, K]) < UpperAll(Grid.Cells[Num, J]) THEN // Ищем строку с минимальным элементом
                K:=J;
          end;
        end;
        IF K <> I THEN
            Exchange(Grid.Rows[I], Grid.Rows[K]) // Если найдена, то меняем ее с верхней :)
    End;
   Grid.Color:=clWindow;
   Progres.visible:=false;
   Grid.Row:=1;
End;
//----------------------------------------------------------------------------------------------------//


//----------------------------------------------------------------------------------------------------//
//Сортировка Stringgrid от большего к наименьшему

//Procedure SortGrid2(Grid: TStringGrid; Num: Integer; Progres:TProgressBar; typ : byte);
//Var
//    I,J,K :   Integer;
//Begin
//  Grid.Color:=clSilver;
//  Progres.Max:=Grid.RowCount;
//  Progres.Position:=0;
//  Progres.visible:=true;
//   FOR I:=1 TO Grid.RowCount-1 DO
//    begin
//      Progres.Position:=Progres.Position+1;
//      Progres.refresh;
//      K:=I;    // Первоначально принимаем, что строка с минимальным элементом - верхняя
//        FOR J:= I+1 TO Grid.RowCount-1 DO
//            IF Grid.Cells[Num, K] < Grid.Cells[Num, J] THEN // Ищем строку с минимальным элементом
//                K:=J;
//        IF K <> I THEN
//            Exchange(Grid.Rows[I], Grid.Rows[K]) // Если найдена, то меняем ее с верхней :)
//    End;
//   Grid.Color:=clWindow;
//   Progres.visible:=false;
//   Grid.Row:=1;
//End;
//----------------------------------------------------------------------------------------------------//


//----------------------------------------------------------------------------------------------------//
//Заполняем массивы
procedure fillarray;
 begin
  namemonth[1]:='Январь';
  namemonth[2]:='Февраль';
  namemonth[3]:='Март';
  namemonth[4]:='Апрель';
  namemonth[5]:='Май';
  namemonth[6]:='Июнь';
  namemonth[7]:='Июль';
  namemonth[8]:='Август';
  namemonth[9]:='Сентябрь';
  namemonth[10]:='Октябрь';
  namemonth[11]:='Ноябрь';
  namemonth[12]:='Декабрь';
  //nameday[1]:='Понедельник';
  //nameday[2]:='Вторник';
  //nameday[3]:='Среда';
  //nameday[4]:='Четверг';
  //nameday[5]:='Пятница';
  //nameday[6]:='Суббота';
  //nameday[7]:='Воскресенье';
  nameday[1]:='ПН';
  nameday[2]:='ВТ';
  nameday[3]:='СР';
  nameday[4]:='ЧТ';
  nameday[5]:='ПТ';
  nameday[6]:='СБ';
  nameday[7]:='ВС';
 end;
//----------------------------------------------------------------------------------------------------//

//----------------------------------------------------------------------------------------------------//
//Возвращает название месяца
function GetMonthName(nM:byte):string;
begin
  fillarray;
  result:=NameMonth[nM];
  exit;
end;
//----------------------------------------------------------------------------------------------------//

//----------------------------------------------------------------------------------------------------//
//Возвращает название дня недели
function GetDayName(nD:byte):string;
begin
  fillarray;
  result:=NameDay[nD];
  exit;
end;
//----------------------------------------------------------------------------------------------------//

//----------------------------------------------------------------------------------------------------//
//Выравнивание формы по центру
procedure CentrForm(Frm: TForm);
begin
     //Frm.Width:=1024;
     Frm.left:=(screen.width div 2) - (frm.Width div 2);
     Frm.Top:=(screen.Height div 2) - (frm.Height div 2);
     //Frm.BorderStyle:=bsSingle;
     //Frm.BorderWidth:=2;
end;
//----------------------------------------------------------------------------------------------------//

//----------------------------------------------------------------------------------------------------//
//Удаляем строки из любого StringGrid
Procedure DelStringGrid(SGrid:TStringGrid;NRow:Integer);
var
    j:integer;
begin
  j:= SGrid.FixedRows + 1;
  If Sgrid.RowCount<j then exit;
  THackGrid(SGrid).DeleteRow(NRow);
end;
//----------------------------------------------------------------------------------------------------//

//----------------------------------------------------------------------------------------------------//
//Удаляем колонки из любого StringGrid
Procedure DelGridColumn(SGrid:TStringGrid;NCol:Integer);
begin
  If Sgrid.ColCount<SGrid.FixedCols+1 then exit;
  THackGrid(SGrid).DeleteCol(NCol);
end;
//----------------------------------------------------------------------------------------------------//


//----------------------------------------------------------------------------------------------------//
//Возвращает количество строк в файле
function kol_row_file(file_name:string):Integer;
var
  List:TStringList;
begin
     List := TStringList.Create;
     try
        List.LoadFromFile(file_name);
        result:=List.Count;
     finally
        List.Free;
     end;
end;
//----------------------------------------------------------------------------------------------------//


//----------------------------------------------------------------------------------------------------//
// Автоподбор ширины столбца
procedure AutoSizeGridColumn(Grid: TStringGrid);
var
  i,icolumn,temp,max: integer;
begin
  for icolumn:=0 to Grid.ColCount - 1 do
   begin
    max := 0;
    for i := 0 to (Grid.RowCount - 1) do
      begin
       temp := Grid.Canvas.TextWidth(grid.cells[icolumn, i]);
       if temp < 350 then
          if temp > max then
             max := temp;
      end;
      Grid.ColWidths[icolumn] := Max + Grid.GridLineWidth + 5;
   end;
end;
//----------------------------------------------------------------------------------------------------//



//----------------------------------------------------------------------------------------------------//
// Процедура обмена текстовых списков (TStrings)
 Procedure Exchange(List1, List2 : TStrings);
 Var
     P1: TStrings;
 begin
     P1:= TStringList.Create;
     P1.Assign(List1);
     List1.Assign(List2);
     List2.Assign(P1);
     P1.Free;
 End;
 //----------------------------------------------------------------------------------------------------//



 //----------------------------------------------------------------------------------------------------//
 //Выделяем фокус ячейки
 procedure SetGridFocus(SGrid: TStringGrid; r, c: integer);
var
  SRect: TGridRect;
begin
  with SGrid do
  begin
    SetFocus; {Передаем фокус сетке}
    Row := r; {Устанавливаем Row/Col}
    Col := c;
    SRect.Top := r; {Определяем выбранную область}
    SRect.Left := c;
    SRect.Bottom := r;
    SRect.Right := c;
    Selection := SRect; {Устанавливаем выбор}
  end;
end;
 //----------------------------------------------------------------------------------------------------//


end.

