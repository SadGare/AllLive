#include <windows.h>
#include <string>

int WINAPI wWinMain(HINSTANCE, HINSTANCE, PWSTR, int) {
    wchar_t path[32768];
    DWORD size = GetModuleFileNameW(nullptr, path, 32768);
    if (!size || size >= 32768) return 1;
    std::wstring root(path);
    root.resize(root.find_last_of(L"\\"));
    std::wstring exe = root + L"\\bundle\\bin\\bridge.exe";
    std::wstring command = L"\"" + exe + L"\"";
    STARTUPINFOW startup{};
    startup.cb = sizeof(startup);
    PROCESS_INFORMATION process{};
    if (!CreateProcessW(exe.c_str(), command.data(), nullptr, nullptr, FALSE,
                        CREATE_NO_WINDOW, nullptr, root.c_str(), &startup, &process)) return 2;
    CloseHandle(process.hThread);
    CloseHandle(process.hProcess);
    return 0;
}
