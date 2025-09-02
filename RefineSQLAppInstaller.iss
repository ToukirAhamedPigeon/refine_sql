; -- RefineSQLApp Installer Script --

[Setup]
AppName=RefineSQLApp
AppVersion=1.0
DefaultDirName={pf}\RefineSQLApp
DefaultGroupName=RefineSQLApp
OutputDir=installer
OutputBaseFilename=RefineSQLApp_Installer
Compression=lzma
SolidCompression=yes
WizardStyle=modern

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Files]
; Copy the entire build folder
Source: "dist\RefineSQLApp\*"; DestDir: "{app}"; Flags: recursesubdirs createallsubdirs ignoreversion

[Icons]
Name: "{group}\RefineSQLApp"; Filename: "{app}\RefineSQLApp.exe"

[Run]
Filename: "{app}\RefineSQLApp.exe"; Description: "Launch RefineSQLApp"; Flags: nowait postinstall skipifsilent
