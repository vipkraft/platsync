program platsync;

{$mode objfpc}{$H+}

uses
  {$IFDEF UNIX}{$IFDEF UseCThreads}
  cthreads,
  {$ENDIF}{$ENDIF}
  Interfaces, // this includes the LCL widgetset
  Forms, main, platproc, datetimectrls, zcomponent, zplain, sync_sprav,
  sync_proc, unit1
  { you can add units after this };

{$R *.res}

begin
  SetHeapTraceOutput('heaptrace.trc');
  RequireDerivedFormResource := True;
  Application.Initialize;
  Application.CreateForm(TForm1, Form1);
  Application.Run;
end.

