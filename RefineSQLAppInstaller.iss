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
ArchitecturesInstallIn64BitMode=x64
PrivilegesRequired=admin
PrivilegesRequiredOverridesAllowed=commandline dialog
SetupIconFile=logo.ico

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "Create a &desktop icon"; GroupDescription: "Additional icons:"; Flags: unchecked

[Files]
; Main application executable
Source: "dist\app.exe"; DestDir: "{app}"; Flags: ignoreversion

; Logo icon for shortcuts
Source: "logo.ico"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
; Start Menu shortcut
Name: "{group}\RefineSQLApp"; Filename: "{app}\app.exe"; IconFilename: "{app}\logo.ico"

; Optional Desktop shortcut
Name: "{commondesktop}\RefineSQLApp"; Filename: "{app}\app.exe"; IconFilename: "{app}\logo.ico"; Tasks: desktopicon

[Run]
Filename: "{app}\app.exe"; Description: "Launch RefineSQLApp"; Flags: nowait postinstall skipifsilent
