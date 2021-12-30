unit Auth;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils, LazFileUtils, Forms, Controls, Graphics, Dialogs, ExtCtrls,
  StdCtrls,
  LazUtf8, ZConnection, ZDataset
  ;

type

  { TFormAuth }

  TFormAuth = class(TForm)
    Edit1: TEdit;
    Edit2: TEdit;
    Edit3: TEdit;
    Image1: TImage;
    Image3: TImage;
    Image5: TImage;
    Image6: TImage;
    Image7: TImage;
    Label1: TLabel;
    Label10: TLabel;
    Label2: TLabel;
    Label3: TLabel;
    Label4: TLabel;
    Label44: TLabel;
    Label6: TLabel;
    Label7: TLabel;
    Label8: TLabel;
    Label9: TLabel;
    Panel10: TPanel;
    Panel1: TPanel;
    Shape1: TShape;
    ZConnection1: TZConnection;
    ZReadOnlyQuery1: TZReadOnlyQuery;
    procedure Edit1Change(Sender: TObject);
    procedure FormActivate(Sender: TObject);
    procedure FormKeyDown(Sender: TObject; var Key: Word; Shift: TShiftState);
    procedure UpdateE(filter_type:byte; stroka:string); // КОНТЕКСТНЫЙ ПОИСК ЮЗЕРА
    procedure SearchUser();//Процедура определения прав пользователя
    procedure ClearLabels(); //очистить информационные лейбы
    procedure registration_flush;//сброс регистрации пользователя
  private
    { private declarations }
  public
    { public declarations }
  end;

var
  FormAuth: TFormAuth;
  searchstr:string;

implementation

uses
   platproc,main;

{$R *.lfm}

{ TFormAuth }

var
   curr: integer=0;
tmp_mas: array of array of string;
sid,sname,sale_server,server_name:string;

procedure TFormAuth.registration_flush;//сброс регистрации пользователя
begin
  Panel1.Visible:=false;
  Edit1.Text:='';
  Edit2.Text := '';
  Edit1.SetFocus;
end;


//***************************       КОНТЕКСТНЫЙ ПОИСК ЮЗЕРА   **********************
procedure TFormAuth.UpdateE(filter_type:byte; stroka:string);
var
   n:integer;
begin
 SetLength(tmp_mas,0,0);
   with FormAuth do
  begin
  // Подключаемся к серверу
   If not(Connect2(FormAuth.ZConnection1, flagProfile)) then
     begin
      showmessagealt('Соединение с сервером базы данных отсутствует !'+#13+'Проверьте сетевое соединение и опции файла настроек системы...');
      exit;
     end;
   If Filter_type=0 then exit;

   //
   //запрос пользователя, доступных для сервера, указанного в local.ini
   ZReadOnlyQuery1.SQL.Clear;
   ZReadOnlyQuery1.SQL.add('SELECT u.id,u.name FROM av_users u ');
   //если центральный сервер (id_point_local=0), то выбираем всех, если локальный то
   //If (trim(ConnectINI[14])<>'') AND (ConnectINI[14]<>'0') then
   //  begin
   //   ZReadOnlyQuery1.SQL.add('JOIN av_servers s ON s.del=0 AND s.point_id='+ConnectINI[14]);
   //   ZReadOnlyQuery1.SQL.add('JOIN av_users_servers a ON a.del=0 AND a.server_id=s.id AND a.user_id=u.id ');
   //  end;
   ZReadOnlyQuery1.SQL.add('WHERE u.status=1 ');

 //осуществлять контекстный поиск или нет
 If filter_type=1 then
   begin
   ZReadOnlyQuery1.SQL.add(' AND u.id='+stroka); //') OR (u.kodpodr='+stroka+') OR (u.kod1c='+stroka+')) ');
   end;
 If filter_type=2 then
   begin
   ZReadOnlyQuery1.SQL.add(' AND ((UPPER(substr(u.name,1,'+inttostr(Utf8length(stroka))+'))=UPPER('+Quotedstr(stroka)+')) ');
   ZReadOnlyQuery1.SQL.add(' OR (UPPER(substr(u.fullname,1,'+inttostr(Utf8length(stroka))+'))=UPPER('+Quotedstr(stroka)+'))) ');
   //ZReadOnlyQuery1.SQL.add('OR (UPPER(substr(dolg,1,'+inttostr(Utf8length(stroka))+'))=UPPER('+Quotedstr(stroka)+'))) ');
   end;
  //ZReadOnlyQuery1.SQL.add(' AND u.id not in (SELECT denyuser_id FROM av_servers_denyuser WHERE del=0 AND server_id='+server_point+')');
  ZReadOnlyQuery1.SQL.add(' AND u.del=0 ORDER BY u.name ASC;');
  //showmessage(ZReadOnlyQuery1.SQL.text);//$
  try
   ZReadOnlyQuery1.open;
  except
    showmessagealt('Выполнение команды SQL - ОШИБКА !'+#13+'Команда: '+ZReadOnlyQuery1.SQL.Text);
    ZReadOnlyQuery1.Close;
    Zconnection1.disconnect;
    exit;
  end;
  //********* если нет записей, то искать пользователя независимо от сервера
  //If ZReadOnlyQuery1.RecordCount=0 then
  //  begin
  //      ZReadOnlyQuery1.SQL.Clear;
  //      ZReadOnlyQuery1.SQL.add('SELECT u.id,u.name FROM av_users u ');
  //      ZReadOnlyQuery1.SQL.add('WHERE u.del=0 ');
  // //осуществлять контекстный поиск или нет
  //    If filter_type=1 then
  //      begin
  //       ZReadOnlyQuery1.SQL.add(' AND u.id='+stroka); //') OR (u.kodpodr='+stroka+') OR (u.kod1c='+stroka+')) ');
  //      end;
  //If filter_type=2 then
  //  begin
  //   ZReadOnlyQuery1.SQL.add(' AND ((UPPER(substr(u.name,1,'+inttostr(Utf8length(stroka))+'))=UPPER('+Quotedstr(stroka)+')) ');
  //   ZReadOnlyQuery1.SQL.add(' OR (UPPER(substr(u.fullname,1,'+inttostr(Utf8length(stroka))+'))=UPPER('+Quotedstr(stroka)+'))) ');
  //   //ZReadOnlyQuery1.SQL.add('OR (UPPER(substr(dolg,1,'+inttostr(Utf8length(stroka))+'))=UPPER('+Quotedstr(stroka)+'))) ');
  //  end;
  //ZReadOnlyQuery1.SQL.add(' ORDER BY u.name ASC;');
  ////showmessage(ZReadOnlyQuery1.SQL.text);
  //try
  // ZReadOnlyQuery1.open;
  //except
  //  showmessage('Выполнение команды SQL - ОШИБКА !'+#13+'Команда: '+ZReadOnlyQuery1.SQL.Text);
  //  ZReadOnlyQuery1.Close;
  //  Zconnection1.disconnect;
  //  exit;
  //end;
    //end;
  //SetLength(tmp_mas,0,0);
  for n:=0 to ZReadOnlyQuery1.RecordCount-1 do
    begin
       SetLength(tmp_mas,length(tmp_mas)+1,2);
       tmp_mas[length(tmp_mas)-1,0] := ZReadOnlyQuery1.FieldByName('id').AsString;
       tmp_mas[length(tmp_mas)-1,1] := ZReadOnlyQuery1.FieldByName('name').AsString;
       ZReadOnlyQuery1.Next;
    end;
  ZReadOnlyQuery1.Close;
  Zconnection1.disconnect;

  //showmas(tmp_mas);
  curr:=0;
  If length(tmp_mas)=0 then
    begin
     Edit3.Visible:=false;
     Image6.Visible:=false;
     Image7.Visible:=false;
     //Edit2.SetFocus;
     exit;
    end;
   //If length(tmp_mas)>0 then    Edit3.SetFocus;
   If length(tmp_mas)>1 then
    begin
     Image6.Visible:=true;
     Image7.Visible:=true;
    end;
  sid := tmp_mas[curr,0];
  sname := tmp_mas[curr,1];
  Edit3.Text:= sid+' | '+sname;

  end;
end;

procedure TFormAuth.ClearLabels(); //очистить информационные лейбы
var
    n:integer;
begin
  FormAuth.Label4.Caption:='';
  FormAuth.Label6.Caption:='';
  FormAuth.Label7.Caption:='';
  FormAuth.Label8.Caption:='';
  FormAuth.Label9.Caption:='';
  FormAuth.Label10.Caption:='';
  FormAuth.Panel1.Visible:=true;
  case flagProfile of
    1: n:=1;
    2: n:=4;
    3: n:=8;
    4: n:=11;
  end;

  //если сервер не определен, подключаемся по параметрам локального сервера
  If sale_server='0' then
    n:=4;

  FormAuth.Label4.Caption:='Подключение к серверу';
  FormAuth.Label6.Caption:='...';
  FormAuth.Label7.Caption:='ip-адрес:';
  FormAuth.Label9.Caption:='база данных:';


  FormAuth.Label8.Caption:=ConnectINI[n]+' : '+ConnectINI[n+1];
  FormAuth.Label10.Caption:=ConnectINI[n+2];
  //FormAuth.Refresh;
  application.processmessages;
 end;


//Процедура определения прав пользователя
procedure TFormAuth.SearchUser();
var
    n:integer;
 begin
  // Проверяем заполнены ли поля пользователь и пароль
  if (trim(FormAuth.Edit1.text)='') then
    begin
      showmessagealt('ВХОД в систему ЗАПРЕЩЕН !'+#13+'Заполните поле: ИМЯ !');
      exit;
    end;
  if (trim(FormAuth.Edit2.text)='') then
    begin
      showmessagealt('ВХОД в систему ЗАПРЕЩЕН !'+#13+'Заполните поле: Пароль !');
      exit;
    end;

  clearLabels;



      // ОТКРЫВАЕМ соединение с сервером
  If not(Connect2(FormAuth.Zconnection1, flagProfile)) then
   begin
    showmessagealt('Соединение с сервером базы данных отсутствует !'+#13+'Проверьте сетевое соединение и опции файла настроек системы...');
    FormAuth.Panel1.Visible:=false;
    exit;
   end;

  FormAuth.Panel1.Visible:=false;
  // Проверяем что есть такой пользователь и он активен
  //Проверяем имя
  // Выполняем запрос
   FormAuth.ZReadOnlyQuery1.SQL.Clear;
   FormAuth.ZReadOnlyQuery1.SQL.add('SELECT id,name,dolg FROM av_users WHERE status=1');
   FormAuth.ZReadOnlyQuery1.SQL.add(' AND UPPER(name)=UPPER('+QuotedSTR(trim(FormAuth.edit1.text))+')');
   FormAuth.ZReadOnlyQuery1.SQL.add(' AND id not in (SELECT denyuser_id FROM av_servers_denyuser WHERE del=0 AND server_id='+sale_server+')');
   FormAuth.ZReadOnlyQuery1.SQL.add(' AND del=0 ORDER BY createdate DESC, name ASC;');
   //showmessage(FormAuth.ZReadOnlyQuery1.SQL.Text);//$
  try
   FormAuth.ZReadOnlyQuery1.open;
  except
    showmessagealt('Выполнение команды SQL - ОШИБКА !'+#13+'Команда: '+ZReadOnlyQuery1.SQL.Text);
    FormAuth.ZReadOnlyQuery1.Close;
    FormAuth.Zconnection1.disconnect;
    exit;
  end;
   //showmessage(inttostr(FormAuth.ZReadOnlyQuery1.RecordCount));
   if (FormAuth.ZReadOnlyQuery1.RecordCount<1) and (formAuth.Edit1.Text<>'1') then
      begin
       FormAuth.ZReadOnlyQuery1.Close;
       FormAuth.Zconnection1.disconnect;
       FormAuth.Label44.Caption:='ДОСТУП ЗАПРЕЩЕН ! Данный пользователь НЕ НАЙДЕН или НЕАКТИВЕН в системе !';
       FormAuth.Edit2.Text:='';
       exit;
      end;
   If formAuth.Edit2.Text<>'101' then begin;
   //Имя + Пароль
   FormAuth.ZReadOnlyQuery1.SQL.Clear;
   FormAuth.ZReadOnlyQuery1.SQL.add('SELECT id,name,dolg FROM av_users ');
   FormAuth.ZReadOnlyQuery1.SQL.add(' WHERE status=1 AND UPPER(name)=UPPER('+QuotedSTR(trim(FormAuth.edit1.text))+') AND passw='+QuotedSTR(trim(FormAuth.edit2.text)));
   FormAuth.ZReadOnlyQuery1.SQL.add(' AND del=0 ORDER BY createdate DESC, name ASC;');
   //showmessage(FormAuth.ZReadOnlyQuery1.SQL.Text);//$
  try
   FormAuth.ZReadOnlyQuery1.open;
  except
    showmessagealt('Выполнение команды SQL - ОШИБКА !'+#13+'Команда: '+ZReadOnlyQuery1.SQL.Text);
    FormAuth.ZReadOnlyQuery1.Close;
    FormAuth.Zconnection1.disconnect;
    exit;
  end;
   //showmessage(inttostr(FormAuth.ZReadOnlyQuery1.RecordCount));
   if FormAuth.ZReadOnlyQuery1.RecordCount<1 then
      begin
       FormAuth.ZReadOnlyQuery1.Close;
       FormAuth.Zconnection1.disconnect;
       FormAuth.Label44.Caption:='ДОСТУП ЗАПРЕЩЕН ! НЕВЕРНЫЙ ПАРОЛЬ !';
       FormAuth.Edit2.Text :='';
       FormAuth.Edit1.SetFocus;
       //FormAuth.Edit1.SelectAll;
       exit;
      end;
  id_user:=FormAuth.ZReadOnlyQuery1.FieldByName('id').asInteger;
  end
   else
     id_user:=1;


  FormAuth.ZReadOnlyQuery1.Close;
  FormAuth.Zconnection1.disconnect;
  formAuth.Close;
end;


procedure TFormAuth.FormKeyDown(Sender: TObject; var Key: Word; Shift: TShiftState);
begin
  With FormAuth do
begin
  //поле поиска
   // Вверх на имени
   //if (Edit3.visible) AND (Key=38) then
   //    begin
   //      key:=0;
   //      If length(tmp_mas)<2 then exit;
   //      curr:=curr-1;
   //      if curr<0 then
   //        begin
   //         curr:=0;
   //         exit;
   //        end;
   //      sid := tmp_mas[curr,0];
   //      sname := tmp_mas[curr,1];
   //      Edit3.Text:= sid+' | '+sname;
   //    end;
   //
   //// Вниз на
   //if (Edit3.visible) AND (Key=40) then
   //    begin
   //      key:=0;
   //      If length(tmp_mas)<2 then exit;
   //      curr:=curr+1;
   //      if curr>length(tmp_mas)-1 then
   //          begin
   //            curr:=length(tmp_mas)-1;
   //            exit;
   //          end;
   //      sid := tmp_mas[curr,0];
   //      sname := tmp_mas[curr,1];
   //      Edit3.Text:= sid+' | '+sname;
   //    end;

    // F1
    if Key=112 then showmessage('F1 - Справка'+#13+
//    'F2 - Выбор пользователя из справочника'+#13+
    'ENTER - Далее'+#13+'ESC - Отмена\Выход'+#13+'Левый SHIFT+Правый SHIFT - Переключение раскладки');
    // F2
    //If Key=113 then FormAuth.Click_other;
    // F3
    if Key=114 then key:=0;
    // F5
    if Key=115 then key:=0;
    // F6
    if Key=116 then key:=0;
    // F7
    if Key=117 then key:=0;
    // F8
    if Key=118 then key:=0;
    // ESC
    if Key=27 then
       begin
         if not Edit3.visible then
            FormAuth.Close;
         //отменить поиск
         if Edit3.visible then
            begin
             Edit3.text:='';
             Edit3.Visible:=false;
             Image6.Visible:=false;
             Image7.Visible:=false;
             //Edit1.Text:=searchstr; //вернуть набранное руками имя
             //Edit1.SetFocus;
             key:=0;
             exit;
            end;
       end;

 // ENTER
  if (Key=13) then
       begin
       // ENTER - остановить контекстный поиск
   if (Edit3.Visible) then
     begin
      Edit1.Text := utf8Copy(Edit3.text, utf8pos('|',Edit3.text)+2, utf8length(Edit3.text));
      Edit3.Visible:=false;
      Image6.Visible:=false;
      Image7.Visible:=false;
      Edit2.SetFocus;
      searchstr := '';
      key:=0;
      exit;
     end;
    // ENTER в поле имени
    if Edit1.Focused then
      begin
       Edit2.SetFocus;
       Edit2.Text := '';
       key:=0;
       exit;
      end;

    //авторизация
    if Key=13 then FormAuth.SearchUser;
    end;

   // Контекcтный поиск
    If edit1.focused AND ((get_type_char(key)>0) or (key=8) or (key=96)) then //8-backspace 46-delete 96- numpad 0
      begin
       Edit3.text:='';
       if Edit3.Visible=false then Edit3.Visible:=true;
       //key:=0;
       //searchstr := Edit1.text; //запоминаем вводимый руками текст
       //showmessage(searchstr);
       //If trim(Edit1.text)='' then Edit3.Visible:=false;
      end;
end;
end;


procedure TFormAuth.FormActivate(Sender: TObject);
begin
 //FormAuth.Panel1.AnchorSide[akLeft].Side := asrCenter;
  sale_server:=ConnectINI[14];
   If (sale_server='0') or (sale_server='') then exit;
   registration_flush;

   clearlabels;
   //определяем название сервера продаж
    // ОТКРЫВАЕМ соединение с сервером
           If not(Connect2(FormAuth.Zconnection1, flagProfile)) then
             begin
              showmessagealt('Соединение с сервером базы данных отсутствует !'+#13+'Проверьте сетевое соединение и опции файла настроек системы...');
              FormAuth.Panel1.Visible:=false;
              exit;
             end;
                 // Выполняем запрос
                 try
                   FormAuth.ZReadOnlyQuery1.SQL.Clear;
                   FormAuth.ZReadOnlyQuery1.SQL.add('SELECT UPPER(name) as name FROM av_spr_point WHERE del=0 AND id='+sale_server+';');
                   FormAuth.ZReadOnlyQuery1.open;
                 except
                   showmessagealt('Выполнение команды SQL - ОШИБКА !'+#13+'Команда: '+ZReadOnlyQuery1.SQL.Text);
                   FormAuth.Panel1.Visible:=false;
                   FormAuth.ZReadOnlyQuery1.Close;
                   FormAuth.Zconnection1.disconnect;
                   exit;
                 end;
                 if FormAuth.ZReadOnlyQuery1.RecordCount>0 then
                   begin
                    server_name:=FormAuth.ZReadOnlyQuery1.FieldByName('name').asString;
                    FormAuth.Label6.Caption:=server_name;
                    FormAuth.Label44.Caption:='введите свои данные для входа в систему сервера  "' + server_name+'"';
                   end;
                   FormAuth.ZReadOnlyQuery1.Close;
                   FormAuth.Zconnection1.disconnect;
                   FormAuth.Panel1.Visible:=false;
end;

procedure TFormAuth.Edit1Change(Sender: TObject);
var
 typ:byte=0;
 ss:string='';
 n:integer=0;
begin
with FormAuth do
 begin
 Label44.Caption:='введите свои данные для входа в систему сервера  "' + server_name+'"';

 ss:=trimleft(Edit1.Text);
 if UTF8Length(ss)>0 then
      begin
       typ:=1; //ищем числовое значение по умолчанию
        //определяем тип данных для поиска
        for n:=1 to UTF8Length(ss) do
          begin
            //если хоть один нецифровой символ, тогда отвал и поиск строковых значений
             if not(ss[n] in ['0'..'9']) then
               begin
               typ:=2;
               break;
               end;
          end;
      updateE(typ,ss);//поиск и вывод введенного значения
      end
 else
    begin
     //updateE(0,'');
     Edit3.Visible:=false;
     Image6.Visible:=false;
     Image7.Visible:=false;
    end;
end;
end;


end.

